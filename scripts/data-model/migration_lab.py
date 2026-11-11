"""
FleetCam — laboratório de migração e modelagem em banco DESCARTÁVEL.

O que faz (tudo em containers efêmeros, nunca no banco da stack):
  1. sobe postgres:16 numa rede Docker isolada;
  2. aplica as migrações Flyway REAIS (imagem flyway/flyway, versão do Spring Boot
     3.3.5) uma versão por vez, V1 -> V11, semeando dados entre versões para que
     os backfills (V9, V10, V11) sejam exercitados com dado pré-existente;
  3. tira snapshot do catálogo após cada versão -> evolução do schema;
  4. verifica regras de integridade multitenant, índices/UNIQUE parciais,
     CHECKs e backfills (falha com exit 1 se alguma verificação falhar);
  5. aplica a PROPOSTA V12 (docs/data-model/proposals, fora do classpath do
     Flyway) com forward + rollback e prova que o rollback volta ao V11;
  6. gera dicionário de dados, DBML e evolução em docs/data-model/generated/.

Uso:
    python scripts/data-model/migration_lab.py            # roda e regrava os artefatos
    python scripts/data-model/migration_lab.py --check    # roda e falha se os artefatos versionados estiverem defasados
Requer: Docker e `pip install psycopg2-binary`.
"""
from __future__ import annotations

import argparse
import json
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))
import schema_tools as st  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "src" / "main" / "resources" / "db" / "migration"
DATA_MODEL = ROOT / "docs" / "data-model"
GENERATED = DATA_MODEL / "generated"
PROPOSAL_UP = DATA_MODEL / "proposals" / "V12__proposed_integrity.sql"
PROPOSAL_DOWN = DATA_MODEL / "proposals" / "V12__proposed_integrity.rollback.sql"
DESCRIPTIONS = DATA_MODEL / "descriptions.json"
PG_IMAGE = "postgres:16"
FLYWAY_IMAGE = "flyway/flyway:10.10.0"  # versão gerenciada pelo Spring Boot 3.3.5


def sh(*args: str, check: bool = True, capture: bool = True) -> str:
    r = subprocess.run(list(args), capture_output=capture, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:4])}... falhou ({r.returncode}): {r.stderr[-800:]}")
    return (r.stdout or "").strip()


class Lab:
    def __init__(self) -> None:
        tag = secrets.token_hex(3)
        self.net = f"fleetcam-dm-{tag}"
        self.pg = f"fleetcam-dm-pg-{tag}"
        self.password = secrets.token_hex(16)  # efêmera, nunca gravada
        self.port: int | None = None
        self.results: list[tuple[str, bool, str]] = []

    # ---------------------------------------------------------------- infra
    def up(self) -> None:
        sh("docker", "network", "create", self.net)
        sh("docker", "run", "-d", "--name", self.pg, "--network", self.net, "--network-alias", "pg",
           "-e", f"POSTGRES_PASSWORD={self.password}", "-e", "POSTGRES_DB=fleetcam",
           "-p", "127.0.0.1::5432", PG_IMAGE)
        self.port = int(sh("docker", "port", self.pg, "5432/tcp").splitlines()[0].rsplit(":", 1)[1])
        deadline = time.time() + 90
        while True:
            try:
                self.connect().close()
                break
            except psycopg2.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(1)

    def down(self) -> None:
        sh("docker", "rm", "-f", self.pg, check=False)
        sh("docker", "network", "rm", self.net, check=False)

    def connect(self):
        conn = psycopg2.connect(host="127.0.0.1", port=self.port, dbname="fleetcam",
                                user="postgres", password=self.password)
        conn.autocommit = True
        return conn

    def flyway_migrate(self, target: int) -> None:
        """Flyway real, sem bind mount (docker cp) — funciona em CI e em pastas sincronizadas."""
        name = f"{self.pg}-fw-{target}"
        sh("docker", "create", "--name", name, "--network", self.net,
           "-e", "FLYWAY_URL=jdbc:postgresql://pg:5432/fleetcam", "-e", "FLYWAY_USER=postgres",
           "-e", f"FLYWAY_PASSWORD={self.password}", "-e", "FLYWAY_LOCATIONS=filesystem:/flyway/sql",
           FLYWAY_IMAGE, f"-target={target}", "migrate")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                sql_dir = Path(tmp) / "sql"
                shutil.copytree(MIGRATIONS, sql_dir)
                sh("docker", "cp", f"{sql_dir}/.", f"{name}:/flyway/sql")
            out = sh("docker", "start", "-a", name)
            if "Successfully applied" not in out and "is up to date" not in out:
                raise RuntimeError(f"Flyway sem confirmação para V{target}: {out[-500:]}")
        finally:
            sh("docker", "rm", "-f", name, check=False)

    # ---------------------------------------------------------------- checks
    def check(self, name: str, ok: bool, detail: str) -> None:
        self.results.append((name, bool(ok), detail))
        print(f"  [{'OK' if ok else 'FALHOU'}] {name} — {detail}", flush=True)

    def expect_error(self, conn, name: str, sql: str, err_cls, params=()) -> None:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or None)
            self.check(name, False, "comando deveria ter sido rejeitado")
        except err_cls as e:
            self.check(name, True, f"rejeitado: {type(e).__name__}")


def q1(conn, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params or None)  # None: '%' literal (ex.: LIKE) não vira placeholder
    return cur.fetchone()[0]


# --------------------------------------------------------------------- seeds
SEED_V1 = """
INSERT INTO devices (tenant_id, id, vehicle_id, model, firmware_version) VALUES
 ('tenant-a','cam-001','truck-01','FC-200','1.0.0'),
 ('tenant-a','cam-002','truck-02','FC-200','1.0.0'),
 ('tenant-b','cam-900','van-09','FC-100','0.9.1');
INSERT INTO ingest_events (tenant_id,event_id,device_id,boot_id,sequence_number,event_kind,payload_hash,payload,occurred_at) VALUES
 ('tenant-a','00000000-0000-0000-0000-000000000001','cam-001','boot-1',1,'device_heartbeat','h1','{"k":"seed-1"}','2026-01-01T00:00:00Z'),
 ('tenant-a','00000000-0000-0000-0000-000000000002','cam-001','boot-1',2,'storage_status','h2','{"k":"seed-2"}','2026-01-01T00:01:00Z'),
 ('tenant-b','00000000-0000-0000-0000-000000000009','cam-900','boot-9',1,'device_heartbeat','h9','{"k":"seed-9"}','2026-01-01T00:00:00Z');
INSERT INTO outbox (tenant_id,event_id,device_id,payload) VALUES
 ('tenant-a','00000000-0000-0000-0000-000000000001','cam-001','{}');
INSERT INTO device_state (tenant_id,device_id,boot_id,sequence_number,occurred_at,storage_state,capture_state) VALUES
 ('tenant-a','cam-001','boot-1',2,'2026-01-01T00:01:00Z','OK','RECORDING');
"""

SEED_V7 = """
-- dead letters ANTES da coluna tenant_id (V8) e do backfill (V9)
INSERT INTO dead_letters (id,topic,kafka_partition,kafka_offset,payload,error) VALUES
 ('10000000-0000-0000-0000-000000000001','fleetcam.events',0,10,'{"k":"seed-9"}','handler error'),
 ('10000000-0000-0000-0000-000000000002','fleetcam.events',0,11,'{"k":"desconhecido"}','handler error'),
 ('10000000-0000-0000-0000-000000000003','fleetcam.events',0,12,'isto nao e json','parse error');
"""

SEED_V10 = """
-- evidência ANTES de upload_requested_at (V11) com created_at no passado
INSERT INTO evidence (id,tenant_id,event_id,object_key,mime_type,byte_size,sha256,origin,created_at) VALUES
 ('20000000-0000-0000-0000-000000000001','tenant-a','00000000-0000-0000-0000-000000000001',
  'tenant-a/cam-001/seed.jpg','image/jpeg',1024,'abc','device','2026-01-02T10:00:00Z');
"""


def run(args: argparse.Namespace) -> int:
    lab = Lab()
    snapshots: dict[int, dict] = {}
    try:
        print("[lab] subindo PostgreSQL descartável...", flush=True)
        lab.up()
        conn = lab.connect()
        print(f"[lab] PostgreSQL {q1(conn, 'SHOW server_version')}; Flyway {FLYWAY_IMAGE}", flush=True)
        versions = sorted(int(p.name.split("__")[0][1:]) for p in MIGRATIONS.glob("V*__*.sql"))
        for v in versions:
            lab.flyway_migrate(v)
            snapshots[v] = st.snapshot(conn)
            if v == 1:
                conn.cursor().execute(SEED_V1)
            elif v == 7:
                conn.cursor().execute(SEED_V7)
            elif v == 10:
                conn.cursor().execute(SEED_V10)
            print(f"[lab] V{v} aplicada", flush=True)
        latest = versions[-1]

        print("[lab] verificações pós-upgrade:", flush=True)
        hist = q1(conn, "SELECT count(*) FROM flyway_schema_history WHERE success AND version IS NOT NULL")
        lab.check("flyway.historico_completo", hist == len(versions), f"{hist} migrações com sucesso de {len(versions)}")
        lab.check("upgrade.dados_preservados",
                  q1(conn, "SELECT count(*) FROM devices") == 3 and q1(conn, "SELECT count(*) FROM ingest_events") == 3,
                  "3 devices e 3 eventos semeados em V1 continuam após V11")
        lab.check("V10.backfill_lifecycle",
                  q1(conn, "SELECT count(*) FROM devices WHERE lifecycle_state='PROVISIONED'") == 3,
                  "devices pré-existentes recebem lifecycle_state='PROVISIONED'")
        lab.check("V7.defaults_device_state",
                  q1(conn, "SELECT sd_error_streak + sd_recovery_streak FROM device_state") == 0,
                  "colunas NOT NULL DEFAULT 0 preenchidas em linha pré-existente")
        lab.check("V9.backfill_tenant_dead_letters",
                  q1(conn, "SELECT tenant_id FROM dead_letters WHERE kafka_offset=10") == "tenant-b"
                  and q1(conn, "SELECT count(*) FROM dead_letters WHERE tenant_id IS NULL") == 2,
                  "só a dead letter cujo payload casa com evento aceito ganha tenant; JSON inválido não quebra a migração")
        lab.check("V11.backfill_upload_requested_at",
                  q1(conn, "SELECT upload_requested_at = created_at FROM evidence") is True,
                  "evidência antiga recebe upload_requested_at = created_at")

        lab.expect_error(conn, "multitenant.fk_composta_bloqueia_cross_tenant",
                         """INSERT INTO ingest_events (tenant_id,event_id,device_id,boot_id,sequence_number,event_kind,
                            payload_hash,payload,occurred_at) VALUES ('tenant-b',gen_random_uuid(),'cam-001','b',1,'x','h','{}',now())""",
                         psycopg2.errors.ForeignKeyViolation)
        lab.expect_error(conn, "idempotencia.unique_sequencia_evento",
                         """INSERT INTO ingest_events (tenant_id,event_id,device_id,boot_id,sequence_number,event_kind,
                            payload_hash,payload,occurred_at) VALUES ('tenant-a',gen_random_uuid(),'cam-001','boot-1',1,
                            'device_heartbeat','h','{}',now())""",
                         psycopg2.errors.UniqueViolation)

        cur = conn.cursor()
        inc = "INSERT INTO incidents (id,tenant_id,device_id,type,status,summary) VALUES (gen_random_uuid(),'tenant-a','cam-001',%s,'OPEN','x')"
        cur.execute(inc, ("CONNECTIVITY",))
        lab.expect_error(conn, "unique_parcial.um_incidente_aberto_por_tipo", inc, psycopg2.errors.UniqueViolation, ("CONNECTIVITY",))
        cur.execute(inc + " ON CONFLICT DO NOTHING", ("CONNECTIVITY",))
        lab.check("unique_parcial.on_conflict_do_nothing_idempotente", cur.rowcount == 0,
                  "o INSERT ... ON CONFLICT DO NOTHING usado pelo app não cria duplicata (rowcount=0)")
        cur.execute(inc, ("STORAGE",))
        lab.check("unique_parcial.outro_tipo_permitido", cur.rowcount == 1, "incidente aberto de outro tipo no mesmo device é aceito")
        cur.execute("UPDATE incidents SET status='CLOSED', closed_at=now() WHERE type='CONNECTIVITY'")
        cur.execute(inc, ("CONNECTIVITY",))
        lab.check("unique_parcial.reabre_apos_fechar", cur.rowcount == 1,
                  "após fechar, um novo incidente do mesmo tipo pode ser aberto (histórico preservado)")

        lab.expect_error(conn, "check.evidence_byte_size_zero",
                         """INSERT INTO evidence (id,tenant_id,event_id,object_key,mime_type,byte_size,sha256,origin)
                            VALUES (gen_random_uuid(),'tenant-a','00000000-0000-0000-0000-000000000001','k0','image/jpeg',0,'x','d')""",
                         psycopg2.errors.CheckViolation)
        lab.expect_error(conn, "check.evidence_byte_size_acima_10MiB",
                         """INSERT INTO evidence (id,tenant_id,event_id,object_key,mime_type,byte_size,sha256,origin)
                            VALUES (gen_random_uuid(),'tenant-a','00000000-0000-0000-0000-000000000001','k1','image/jpeg',10485761,'x','d')""",
                         psycopg2.errors.CheckViolation)
        lab.expect_error(conn, "check.lifecycle_state_dominio",
                         "UPDATE devices SET lifecycle_state='ROUBADO' WHERE id='cam-001'", psycopg2.errors.CheckViolation)

        # índice parcial do outbox: 20.000 entregues + 5 pendentes, plano real sem forçar nada
        cur.execute("""
            INSERT INTO ingest_events (tenant_id,event_id,device_id,boot_id,sequence_number,event_kind,payload_hash,payload,occurred_at)
            SELECT 'tenant-a', gen_random_uuid(), 'cam-002', 'boot-bulk', g, 'device_heartbeat', 'h', '{}', now()
            FROM generate_series(1, 20005) g;
            INSERT INTO outbox (tenant_id,event_id,device_id,payload,delivered_at)
            SELECT tenant_id, event_id, device_id, '{}', CASE WHEN sequence_number <= 20000 THEN now() END
            FROM ingest_events WHERE boot_id='boot-bulk';
            ANALYZE outbox;""")
        cur.execute("""EXPLAIN (FORMAT JSON) SELECT tenant_id,event_id,device_id,payload,traceparent
                       FROM outbox WHERE delivered_at IS NULL ORDER BY created_at LIMIT 50""")
        plan = json.dumps(cur.fetchone()[0])
        lab.check("indice_parcial.outbox_pending_usado_pelo_poll", "outbox_pending" in plan,
                  "consulta de polling do app (FleetCam.kt) usa outbox_pending")
        cur.execute("CREATE INDEX lab_outbox_full ON outbox (created_at)")
        part = q1(conn, "SELECT pg_relation_size('outbox_pending')")
        full = q1(conn, "SELECT pg_relation_size('lab_outbox_full')")
        cur.execute("DROP INDEX lab_outbox_full")
        lab.check("indice_parcial.outbox_menor_que_indice_total", part < full,
                  f"parcial={part // 1024} KiB vs índice total equivalente={full // 1024} KiB (20.006 linhas, 6 pendentes)")

        # proposta V12: forward -> verificação -> rollback -> igual ao V11
        before = st.snapshot(conn)
        cur.execute(PROPOSAL_UP.read_text(encoding="utf-8"))
        after = st.snapshot(conn)
        lab.check("proposta_v12.aplica_sobre_v11", st.diff(before, after) != [],
                  f"{len(st.diff(before, after))} mudanças aplicadas")
        lab.check("proposta_v12.constraints_validadas",
                  q1(conn, "SELECT count(*) FROM pg_constraint WHERE conname LIKE 'ck_%_status' AND NOT convalidated") == 0,
                  "CHECKs criados NOT VALID e depois validados")
        lab.expect_error(conn, "proposta_v12.rejeita_status_invalido",
                         "UPDATE incidents SET status='ABERTO' WHERE type='STORAGE'", psycopg2.errors.CheckViolation)
        cur.execute(PROPOSAL_UP.read_text(encoding="utf-8"))
        lab.check("proposta_v12.idempotente", st.diff(after, st.snapshot(conn)) == [], "reaplicação não muda nada")
        cur.execute(PROPOSAL_DOWN.read_text(encoding="utf-8"))
        lab.check("proposta_v12.rollback_restaura_v11", st.diff(before, st.snapshot(conn)) == [],
                  "rollback volta exatamente ao snapshot V11")
        conn.close()
    finally:
        if not args.keep:
            lab.down()

    # ------------------------------------------------------------ artefatos
    descriptions = json.loads(DESCRIPTIONS.read_text(encoding="utf-8"))
    final = snapshots[latest]
    dict_csv, dict_errors = st.dictionary(final, descriptions)
    for e in dict_errors:
        lab.check("dicionario.cobertura", False, e)
    if not dict_errors:
        lab.check("dicionario.cobertura", True,
                  f"{sum(len(t['columns']) for t in final['tables'].values())} colunas, 100% descritas e classificadas")

    evo = ["# Evolução do schema — gerado pelo laboratório (Flyway real, catálogo PostgreSQL)", "",
           "Não editar à mão: `python scripts/data-model/migration_lab.py`.", ""]
    prev = {"tables": {}}
    for v in sorted(snapshots):
        name = next(p.name for p in MIGRATIONS.glob(f"V{v}__*.sql"))
        evo.append(f"## {name}")
        evo.append("")
        evo += [f"- `{line}`" for line in st.diff(prev, snapshots[v])] or ["- (sem mudança estrutural: migração de dados)"]
        evo.append("")
        prev = snapshots[v]
    v1, vl = snapshots[versions[0]], final
    evo += [f"## Resumo V{versions[0]} → V{latest}", "",
            "| | V1 | atual |", "|---|---:|---:|",
            f"| tabelas | {len(v1['tables'])} | {len(vl['tables'])} |",
            f"| colunas | {sum(len(t['columns']) for t in v1['tables'].values())} | {sum(len(t['columns']) for t in vl['tables'].values())} |",
            f"| FKs | {sum(len(t['foreign_keys']) for t in v1['tables'].values())} | {sum(len(t['foreign_keys']) for t in vl['tables'].values())} |",
            f"| CHECKs | {sum(len(t['checks']) for t in v1['tables'].values())} | {sum(len(t['checks']) for t in vl['tables'].values())} |",
            f"| índices (fora PK/UNIQUE) | {sum(len(t['indexes']) for t in v1['tables'].values())} | {sum(len(t['indexes']) for t in vl['tables'].values())} |",
            ""]
    artifacts = {
        GENERATED / "schema-v1.json": st.dump(v1),
        GENERATED / f"schema-v{latest}.json": st.dump(vl),
        GENERATED / "schema-evolution.md": "\n".join(evo),
        DATA_MODEL / "dictionary.csv": dict_csv,
        GENERATED / "reverse-engineered.dbml": st.to_dbml(vl),
    }
    stale = []
    for path, content in artifacts.items():
        if args.check:
            cur_txt = path.read_text(encoding="utf-8") if path.exists() else ""
            if cur_txt != content:
                stale.append(path.relative_to(ROOT).as_posix())
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8", newline="\n")
    for s in stale:
        lab.check("artefatos.atualizados", False, f"{s} defasado — rode migration_lab.py e versione")

    report = {"results": [{"check": n, "passed": ok, "detail": d} for n, ok, d in lab.results]}
    if args.report:
        args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    failed = [n for n, ok, _ in lab.results if not ok]
    print(f"\n[lab] {len(lab.results) - len(failed)}/{len(lab.results)} verificações OK", flush=True)
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Laboratório de migração/modelagem do FleetCam")
    p.add_argument("--check", action="store_true", help="falha se artefatos versionados estiverem defasados")
    p.add_argument("--keep", action="store_true", help="não remove os containers ao final")
    p.add_argument("--report", type=Path, help="grava o resultado das verificações em JSON")
    return run(p.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
