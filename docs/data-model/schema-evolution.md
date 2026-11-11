# Evolução do schema (Flyway V1 → V11)

A lista completa, migração a migração, é **gerada** a partir do catálogo pelo
laboratório: [`generated/schema-evolution.md`](generated/schema-evolution.md).

## Resumo medido

| | V1 | V11 |
|---|---:|---:|
| tabelas | 4 | 11 |
| colunas | 34 | 100 |
| FKs | 3 | 7 |
| CHECKs | 0 | 2 |
| índices (fora PK/UNIQUE) | 2 | 7 |

## Linha do tempo

| Versão | Tipo de mudança | Compatível com o código anterior? |
|---|---|---|
| V1 initial | criação: devices, ingest_events, outbox, device_state | — |
| V2 incidents | 3 tabelas novas + UNIQUE parcial | sim (aditiva) |
| V3 evidence | tabela nova com CHECK de tamanho | sim |
| V4 dead_letters | 2 tabelas novas | sim |
| V5 outbox_trace | coluna anulável | sim |
| V6 boot_sessions | tabela nova | sim |
| V7 storage_hysteresis | 3 colunas (2 NOT NULL **com default**) | sim |
| V8 dlt_tenant | coluna anulável + índice | sim |
| V9 backfill_dlt_tenant | **só dados** (UPDATE condicional) | sim |
| V10 lifecycle | coluna NOT NULL com default + CHECK | sim |
| V11 evidence_reconciliation | 2 colunas + backfill + índice parcial | sim |

Todas as 11 migrações são **aditivas** (expand): nenhuma remove ou renomeia
coluna. O upgrade V1 → V11 com dados pré-existentes é testado em banco descartável
pelo laboratório (`scripts/data-model/migration_lab.py`): dados semeados em V1, V7
e V10 sobrevivem e os backfills de V9, V10 e V11 produzem o valor esperado.

## Regras para a próxima versão

1. Nunca editar V1..V11 (o checksum do Flyway quebraria em todo ambiente já migrado).
2. Confirmar que não existe V12 antes de criar.
3. Mudança incompatível = expand/contract em versões separadas.
4. Rodar `python scripts/data-model/migration_lab.py` e versionar os artefatos gerados.
