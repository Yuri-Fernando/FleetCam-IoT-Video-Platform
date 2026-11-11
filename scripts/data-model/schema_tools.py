"""
Ferramentas de catálogo do FleetCam (sem Docker): snapshot do schema PostgreSQL
via information_schema/pg_catalog, diff entre snapshots, dicionário de dados e
DBML. Usadas por migration_lab.py e testadas por test_schema_tools.py.
"""
from __future__ import annotations

import csv
import io
import json

IGNORED_TABLES = {"flyway_schema_history"}


def snapshot(conn, schema: str = "public") -> dict:
    """Extrai o schema do CATÁLOGO (nunca do texto das migrações)."""
    cur = conn.cursor()
    model: dict = {"tables": {}}
    cur.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema=%s AND table_type='BASE TABLE' ORDER BY table_name""", (schema,))
    for (t,) in cur.fetchall():
        if t in IGNORED_TABLES:
            continue
        cur.execute("""SELECT column_name, data_type, is_nullable, column_default
                       FROM information_schema.columns WHERE table_schema=%s AND table_name=%s
                       ORDER BY ordinal_position""", (schema, t))
        cols = [{"name": c, "type": dt, "nullable": n == "YES", "default": d} for c, dt, n, d in cur.fetchall()]
        model["tables"][t] = {"columns": cols, "primary_key": [], "foreign_keys": [], "unique": [],
                              "checks": [], "indexes": []}
    cur.execute("""
        SELECT rel.relname, con.contype, con.conname, pg_get_constraintdef(con.oid),
               ARRAY(SELECT a.attname FROM unnest(con.conkey) WITH ORDINALITY k(n,o)
                     JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k.n ORDER BY k.o),
               frel.relname,
               ARRAY(SELECT a.attname FROM unnest(con.confkey) WITH ORDINALITY k(n,o)
                     JOIN pg_attribute a ON a.attrelid=con.confrelid AND a.attnum=k.n ORDER BY k.o)
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid=con.conrelid
        JOIN pg_namespace ns ON ns.oid=rel.relnamespace
        LEFT JOIN pg_class frel ON frel.oid=con.confrelid
        WHERE ns.nspname=%s ORDER BY rel.relname, con.conname""", (schema,))
    for rel, ctype, cname, cdef, cols, ref, ref_cols in cur.fetchall():
        t = model["tables"].get(rel)
        if t is None:
            continue
        if ctype == "p":
            t["primary_key"] = list(cols)
        elif ctype == "f":
            t["foreign_keys"].append({"columns": list(cols), "ref_table": ref, "ref_columns": list(ref_cols)})
        elif ctype == "u":
            t["unique"].append(list(cols))
        elif ctype == "c":
            t["checks"].append(cdef)
    cur.execute("""
        SELECT t.relname, i.relname, pg_get_indexdef(ix.indexrelid), ix.indisunique,
               pg_get_expr(ix.indpred, ix.indrelid)
        FROM pg_index ix
        JOIN pg_class t ON t.oid=ix.indrelid JOIN pg_class i ON i.oid=ix.indexrelid
        JOIN pg_namespace ns ON ns.oid=t.relnamespace
        LEFT JOIN pg_constraint c ON c.conindid=ix.indexrelid
        WHERE ns.nspname=%s AND c.oid IS NULL ORDER BY t.relname, i.relname""", (schema,))
    for rel, iname, idef, unique, pred in cur.fetchall():
        if rel in model["tables"]:
            model["tables"][rel]["indexes"].append(
                {"name": iname, "definition": idef, "unique": bool(unique), "predicate": pred})
    for t in model["tables"].values():
        t["foreign_keys"].sort(key=lambda f: f["columns"])
        t["unique"].sort()
        t["checks"].sort()
    return model


def dump(model: dict) -> str:
    return json.dumps(model, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def diff(old: dict, new: dict) -> list[str]:
    """Lista legível de mudanças old -> new (ordem determinística)."""
    out: list[str] = []
    ot, nt = old["tables"], new["tables"]
    for t in sorted(set(nt) - set(ot)):
        out.append(f"+ tabela {t}")
    for t in sorted(set(ot) - set(nt)):
        out.append(f"- tabela {t}")
    for t in sorted(set(ot) & set(nt)):
        oc = {c["name"]: c for c in ot[t]["columns"]}
        nc = {c["name"]: c for c in nt[t]["columns"]}
        for c in [x["name"] for x in nt[t]["columns"] if x["name"] not in oc]:
            col = nc[c]
            out.append(f"+ coluna {t}.{c} {col['type']} {'NULL' if col['nullable'] else 'NOT NULL'}"
                       + (f" DEFAULT {col['default']}" if col["default"] else ""))
        for c in sorted(set(oc) - set(nc)):
            out.append(f"- coluna {t}.{c}")
        for c in sorted(set(oc) & set(nc)):
            if oc[c]["type"] != nc[c]["type"]:
                out.append(f"~ tipo {t}.{c}: {oc[c]['type']} -> {nc[c]['type']}")
            if oc[c]["nullable"] != nc[c]["nullable"]:
                out.append(f"~ nulidade {t}.{c}: {'NULL' if oc[c]['nullable'] else 'NOT NULL'} -> "
                           f"{'NULL' if nc[c]['nullable'] else 'NOT NULL'}")
        for kind in ("checks",):
            for x in sorted(set(nt[t][kind]) - set(ot[t][kind])):
                out.append(f"+ check {t}: {x}")
            for x in sorted(set(ot[t][kind]) - set(nt[t][kind])):
                out.append(f"- check {t}: {x}")
        oi = {i["name"]: i for i in ot[t]["indexes"]}
        ni = {i["name"]: i for i in nt[t]["indexes"]}
        for i in sorted(set(ni) - set(oi)):
            out.append(f"+ índice {ni[i]['definition']}")
        for i in sorted(set(oi) - set(ni)):
            out.append(f"- índice {t}.{i}")
        ofk = {json.dumps(f, sort_keys=True) for f in ot[t]["foreign_keys"]}
        nfk = {json.dumps(f, sort_keys=True) for f in nt[t]["foreign_keys"]}
        for f in sorted(nfk - ofk):
            fk = json.loads(f)
            out.append(f"+ fk {t}({','.join(fk['columns'])}) -> {fk['ref_table']}({','.join(fk['ref_columns'])})")
        for f in sorted(ofk - nfk):
            fk = json.loads(f)
            out.append(f"- fk {t}({','.join(fk['columns'])}) -> {fk['ref_table']}")
    return out


DICT_HEADER = ["table", "column", "type", "nullable", "default", "is_pk", "fk_reference",
               "description", "classification"]
CLASSIFICATIONS = {"PUBLICO", "INTERNO", "CONFIDENCIAL", "PESSOAL", "SEGREDO_TECNICO"}


def dictionary(model: dict, descriptions: dict) -> tuple[str, list[str]]:
    errors: list[str] = []
    rows = []
    descriptions = {k: v for k, v in descriptions.items() if not k.startswith("_")}
    for t in sorted(model["tables"]):
        tab = model["tables"][t]
        desc_t = descriptions.get(t)
        if desc_t is None:
            errors.append(f"tabela sem descrição: {t}")
            desc_t = {}
        fks = {}
        for fk in tab["foreign_keys"]:
            for c, rc in zip(fk["columns"], fk["ref_columns"]):
                fks.setdefault(c, []).append(f"{fk['ref_table']}.{rc}")
        for c in tab["columns"]:
            d = desc_t.get(c["name"])
            if not d or not d[0].strip():
                errors.append(f"coluna sem descrição: {t}.{c['name']}")
                d = ["", ""]
            if d[1] not in CLASSIFICATIONS:
                errors.append(f"classificação inválida em {t}.{c['name']}: {d[1]}")
            rows.append([t, c["name"], c["type"], "YES" if c["nullable"] else "NO", c["default"] or "",
                         "YES" if c["name"] in tab["primary_key"] else "NO", " | ".join(fks.get(c["name"], [])),
                         d[0], d[1]])
        for c in sorted(set(desc_t) - {c["name"] for c in tab["columns"]}):
            errors.append(f"descrição sem coluna física: {t}.{c}")
    for t in sorted(set(descriptions) - set(model["tables"])):
        errors.append(f"descrição sem tabela física: {t}")
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(DICT_HEADER)
    w.writerows(rows)
    return buf.getvalue(), errors


def to_dbml(model: dict) -> str:
    out = ["// Gerado por scripts/data-model/migration_lab.py a partir do catálogo do PostgreSQL",
           "// após aplicar TODAS as migrações Flyway. Não editar à mão.", ""]
    refs = []
    for t in sorted(model["tables"]):
        tab = model["tables"][t]
        out.append(f"Table {t} {{")
        for c in tab["columns"]:
            attrs = []
            if len(tab["primary_key"]) == 1 and c["name"] == tab["primary_key"][0]:
                attrs.append("pk")
            elif not c["nullable"]:
                attrs.append("not null")
            out.append(f"  {c['name']} {c['type'].replace(' ', '_')}" + (f" [{', '.join(attrs)}]" if attrs else ""))
        idx = []
        if len(tab["primary_key"]) > 1:
            idx.append(f"    ({', '.join(tab['primary_key'])}) [pk]")
        for u in tab["unique"]:
            idx.append(f"    ({', '.join(u)}) [unique]")
        if idx:
            out += ["  indexes {", *idx, "  }"]
        out += ["}", ""]
        for fk in tab["foreign_keys"]:
            if len(fk["columns"]) == 1:
                refs.append(f"Ref: {t}.{fk['columns'][0]} > {fk['ref_table']}.{fk['ref_columns'][0]}")
            else:
                refs.append(f"Ref: {t}.({', '.join(fk['columns'])}) > {fk['ref_table']}.({', '.join(fk['ref_columns'])})")
    return "\n".join(out + sorted(refs)) + "\n"
