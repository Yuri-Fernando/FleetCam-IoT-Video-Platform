# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/).
Registro de validação por data: [docs/validation.md](docs/validation.md).

## [0.3] — 2026-11-10

### Added
- `docs/data-model/`: modelo conceitual, lógico e físico; dicionário de dados com
  100 colunas (extraído do catálogo PostgreSQL após V11 + `descriptions.json` com
  descrição e classificação); revisão de DDL (achados R1–R7); evolução V1→V11.
- `scripts/data-model/migration_lab.py`: aplica as migrações com **Flyway real**
  (`flyway/flyway:10.10.0`, versão do Spring Boot 3.3.5) uma a uma em `postgres:16`
  descartável, semeia dados entre versões e roda 23 verificações (backfills V9/V10/V11,
  FK composta multitenant, UNIQUE de idempotência, UNIQUE parcial de incidentes,
  CHECKs, índice parcial do outbox, proposta V12 com rollback, cobertura do dicionário).
- `scripts/data-model/schema_tools.py` + `test_schema_tools.py` (8 testes sem Docker).
- `docs/data-model/proposals/V12__proposed_integrity.sql` e rollback — fora do
  classpath do Flyway, **não** aplicados pelo app.
- CI: job `data-model` (`migration_lab.py --check`).

### Unchanged (de propósito)
- Migrações V1..V11 e código Kotlin.

## [0.2] — 2026-10-29
Reconciliação de upload interrompido (ADR 002); lag do consumidor e contador de
reentregas; `crash_check` com SIGKILL; commit do outbox por lote (ADR 003);
correção do fallback IPv6 dos clientes; DNS dinâmico no nginx do console; README e
licença MIT.

## [0.1] — 2026-10-27..29
Núcleo Kotlin/Spring, outbox, Kafka, projeção idempotente, DLT/redrive, saúde de
device e incidentes, evidências S3, adapter VisionBrain, console React,
Prometheus/Grafana/Jaeger, 15 checks de integração.
