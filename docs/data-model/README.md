# Modelagem de dados — FleetCam

Segunda evidência de modelagem do portfólio (a principal é o
[RetentIQ](https://github.com/Yuri-Fernando/retentiq)): aqui o foco é **modelo
operacional multitenant** com migrações versionadas, não warehouse analítico.

| Documento | Conteúdo |
|---|---|
| [conceptual.md](conceptual.md) | entidades e regras de negócio |
| [logical.md](logical.md) | PKs/FKs compostas, cardinalidades, grão por tabela |
| [physical.md](physical.md) | tipos, restrições, **índice parcial do outbox** e **UNIQUE parcial de incidentes** com medição |
| [ddl-review.md](ddl-review.md) | revisão V1..V11: pontos fortes, achados R1–R7 |
| [schema-evolution.md](schema-evolution.md) | V1 → V11 e regras de evolução |
| [dictionary.csv](dictionary.csv) | 100 colunas, gerado do catálogo + [descriptions.json](descriptions.json) |
| [generated/](generated/) | snapshots V1/V11, evolução migração a migração, DBML — **gerados** |
| [proposals/](proposals/) | proposta V12 (CHECKs de status, índice de FK, FK faltante) + rollback — **não** aplicada pelo app |

## Laboratório reproduzível

```bash
pip install psycopg2-binary
python scripts/data-model/migration_lab.py          # requer Docker
python scripts/data-model/migration_lab.py --check  # usado no CI
python -m unittest discover -s scripts/data-model   # sem Docker
```

Sobe `postgres:16` e `flyway/flyway:10.10.0` (mesma versão do Spring Boot 3.3.5)
em rede Docker isolada, aplica V1→V11 uma a uma com dados semeados entre versões,
roda **23 verificações** e remove os containers. Última execução local
(2026-11-10): 23/23 OK.

As migrações históricas não foram tocadas e nenhum código Kotlin foi alterado.
