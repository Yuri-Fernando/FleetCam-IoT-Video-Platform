# Evolução do schema — gerado pelo laboratório (Flyway real, catálogo PostgreSQL)

Não editar à mão: `python scripts/data-model/migration_lab.py`.

## V1__initial.sql

- `+ tabela device_state`
- `+ tabela devices`
- `+ tabela ingest_events`
- `+ tabela outbox`

## V2__incidents.sql

- `+ tabela incident_timeline`
- `+ tabela incidents`
- `+ tabela state_transitions`

## V3__evidence.sql

- `+ tabela evidence`

## V4__dead_letters.sql

- `+ tabela audit_log`
- `+ tabela dead_letters`

## V5__outbox_trace.sql

- `+ coluna outbox.traceparent text NULL`

## V6__boot_sessions.sql

- `+ tabela device_boot_sessions`

## V7__storage_hysteresis.sql

- `+ coluna device_state.sd_last_errors bigint NULL`
- `+ coluna device_state.sd_error_streak integer NOT NULL DEFAULT 0`
- `+ coluna device_state.sd_recovery_streak integer NOT NULL DEFAULT 0`

## V8__dlt_tenant.sql

- `+ coluna dead_letters.tenant_id text NULL`
- `+ índice CREATE INDEX dead_letters_tenant ON public.dead_letters USING btree (tenant_id, created_at DESC)`

## V9__backfill_dlt_tenant.sql

- (sem mudança estrutural: migração de dados)

## V10__lifecycle.sql

- `+ coluna devices.lifecycle_state text NOT NULL DEFAULT 'PROVISIONED'::text`
- `+ check devices: CHECK ((lifecycle_state = ANY (ARRAY['PROVISIONED'::text, 'ACTIVE'::text, 'MAINTENANCE'::text, 'DECOMMISSIONED'::text])))`

## V11__evidence_reconciliation.sql

- `+ coluna evidence.failure_reason text NULL`
- `+ coluna evidence.upload_requested_at timestamp with time zone NOT NULL DEFAULT now()`
- `+ índice CREATE INDEX evidence_pending_requested ON public.evidence USING btree (upload_requested_at) WHERE (status = 'PENDING'::text)`

## Resumo V1 → V11

| | V1 | atual |
|---|---:|---:|
| tabelas | 4 | 11 |
| colunas | 34 | 100 |
| FKs | 3 | 7 |
| CHECKs | 0 | 2 |
| índices (fora PK/UNIQUE) | 2 | 7 |
