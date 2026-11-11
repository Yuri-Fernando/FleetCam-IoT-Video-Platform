# Modelo físico — FleetCam (PostgreSQL 16 + Flyway)

> Fonte da verdade: migrações em `src/main/resources/db/migration/V1..V11`.
> Snapshot do catálogo após V11: [`generated/schema-v11.json`](generated/schema-v11.json).

## Tipos

| Uso | Tipo | Motivo |
|---|---|---|
| identificadores de negócio (`tenant_id`, `device_id`, `boot_id`) | `text` | vêm do dispositivo/credencial; sem limite artificial |
| identificadores gerados | `uuid` (eventos, incidentes, evidências) · `bigserial` (históricos) | UUID gerado no dispositivo permite idempotência sem coordenação |
| instantes | `timestamptz` | dispositivos em fusos diferentes; tudo normalizado para UTC |
| payload | `jsonb` em `ingest_events` (consultável) · `text` em `outbox`/`dead_letters` (bytes exatos a publicar/inspecionar) | `dead_letters.payload` pode nem ser JSON válido — por isso V9 usa `pg_input_is_valid` |
| contadores | `integer`/`bigint` com `DEFAULT 0` | — |

## Restrições

| Tipo | Onde |
|---|---|
| PK composta com `tenant_id` | `devices`, `ingest_events`, `outbox`, `device_state`, `device_boot_sessions` |
| FK composta | 6 das 7 FKs incluem `tenant_id` (a exceção é `incident_timeline.incident_id → incidents.id`) |
| UNIQUE de idempotência | `ingest_events` (5 colunas), `dead_letters` (tópico/partição/offset), `evidence.object_key` |
| UNIQUE **parcial** | `one_open_incident_per_kind` (`WHERE status='OPEN'`) |
| CHECK | `evidence.byte_size` (1 B..10 MiB) · `devices.lifecycle_state` (4 valores, V10) |

## Índices parciais — dois exemplos concretos

### 1. `outbox_pending ON outbox (created_at) WHERE delivered_at IS NULL`

O publicador faz polling com
`SELECT ... FROM outbox WHERE delivered_at IS NULL ORDER BY created_at LIMIT 50`
(`FleetCam.kt`). Em regime normal quase tudo já foi entregue: um índice comum em
`created_at` indexaria todas as linhas para servir as poucas pendentes.

Medido no laboratório (20.006 linhas no outbox, 6 pendentes):

| Índice | Tamanho |
|---|---|
| parcial `outbox_pending` | **16 KiB** |
| índice comum equivalente em `created_at` | 152 KiB |

E o planejador usa o índice parcial na consulta real do app sem nenhuma dica
(`indice_parcial.outbox_pending_usado_pelo_poll`). O índice também só é mantido
quando a linha entra/sai do conjunto pendente, então a marcação de entrega
remove a linha dele.

### 2. `one_open_incident_per_kind` — UNIQUE parcial em incidentes

`UNIQUE (tenant_id, device_id, type) WHERE status = 'OPEN'`

- Heartbeats perdidos chegam em rajada e vários workers podem tentar abrir o
  mesmo incidente ao mesmo tempo. O app faz
  `INSERT ... ON CONFLICT DO NOTHING`: o **banco** garante que só um vence, sem
  lock aplicativo nem `SELECT` antes do `INSERT` (que teria condição de corrida).
- Um UNIQUE comum em (tenant, device, type) impediria **reabrir** um incidente do
  mesmo tipo depois de fechado, ou obrigaria apagar o histórico. O parcial só olha
  as linhas abertas.

Verificado no laboratório: segundo OPEN rejeitado (`UniqueViolation`);
`ON CONFLICT DO NOTHING` retorna 0 linhas; outro tipo é aceito; após fechar, um
novo do mesmo tipo é aceito.

## O que acontece se o schema mudar sem migração

- O app roda Flyway na subida: alteração manual no banco não aparece no histórico
  `flyway_schema_history`, e a próxima migração pode falhar (ex.: coluna já existe)
  ou, pior, passar e deixar ambientes diferentes entre si.
- Remover a FK composta "para acelerar" removeria a única barreira de banco contra
  mistura de tenants.
- Remover o UNIQUE parcial transformaria uma rajada de heartbeats em incidentes
  duplicados.
- **Controle:** `migration_lab.py --check` no CI reconstrói o schema do zero e falha
  se os artefatos versionados divergirem.
