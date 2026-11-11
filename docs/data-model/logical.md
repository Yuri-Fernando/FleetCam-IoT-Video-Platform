# Modelo lógico — FleetCam (operacional, multitenant)

> Extraído do catálogo do PostgreSQL após aplicar V1..V11 (não de leitura manual
> das migrações). DBML: [`generated/reverse-engineered.dbml`](generated/reverse-engineered.dbml).
> Dicionário: [`dictionary.csv`](dictionary.csv) (100 colunas).

## Chaves e o padrão multitenant

| Tabela | PK | FKs | Unicidade |
|---|---|---|---|
| `devices` | (`tenant_id`, `id`) | — | — |
| `ingest_events` | (`tenant_id`, `event_id`) | (`tenant_id`,`device_id`) → `devices` | (`tenant_id`,`device_id`,`boot_id`,`sequence_number`,`event_kind`) |
| `outbox` | (`tenant_id`, `event_id`) | (`tenant_id`,`event_id`) → `ingest_events` | — |
| `device_state` | (`tenant_id`, `device_id`) | (`tenant_id`,`device_id`) → `devices` | — |
| `device_boot_sessions` | (`tenant_id`,`device_id`,`boot_id`) | (`tenant_id`,`device_id`) → `devices` | — |
| `incidents` | `id` (uuid) | (`tenant_id`,`device_id`) → `devices` | **parcial**: (`tenant_id`,`device_id`,`type`) WHERE `status='OPEN'` |
| `incident_timeline` | `id` | `incident_id` → `incidents` | — |
| `evidence` | `id` (uuid) | (`tenant_id`,`event_id`) → `ingest_events` | `object_key` |
| `state_transitions` | `id` | **nenhuma** (achado R3) | — |
| `dead_letters` | `id` (uuid) | — | (`topic`,`kafka_partition`,`kafka_offset`) |
| `audit_log` | `id` | — | — |

### Por que `tenant_id` entra na PK e nas FKs

- `devices.id` só é único **dentro** do tenant (dois clientes podem ter `cam-001`).
- Com FK **composta** `(tenant_id, device_id) → devices(tenant_id, id)`, o banco
  rejeita um evento do tenant B apontando para um dispositivo do tenant A — mesmo
  que o código tenha um bug de autorização. Verificado no laboratório
  (`multitenant.fk_composta_bloqueia_cross_tenant` → `ForeignKeyViolation`).
- Uma FK simples para `devices(id)` exigiria `id` globalmente único e não
  impediria mistura de tenants.

**Exceções:** `incidents`, `evidence` e `dead_letters` usam UUID como PK
(globalmente único) — o isolamento continua garantido pela FK composta
(`incidents`, `evidence`) ou é parcial por desenho (`dead_letters.tenant_id`
anulável: uma mensagem que nem chegou a ser interpretada pode não ter tenant).

## Cardinalidades

- tenant 1:N dispositivo · dispositivo 1:N evento · evento 1:0..1 outbox
- dispositivo 1:1 estado atual · dispositivo 1:N sessões de boot
- dispositivo 1:N incidentes, **no máximo 1 aberto por tipo**
- incidente 1:N anotações · evento 1:N evidências

## Grão e natureza das tabelas

| Tabela | Natureza | Grão |
|---|---|---|
| `ingest_events` | log imutável (append-only) | 1 evento aceito |
| `outbox` | fila transacional | 1 publicação pendente/entregue |
| `device_state` | projeção (upsert) | 1 dispositivo |
| `state_transitions`, `incident_timeline`, `audit_log` | histórico append-only | 1 mudança/anotação/ação |
| `incidents`, `evidence` | entidades com ciclo de vida (status) | 1 incidente / 1 objeto |

Diferente do RetentIQ (warehouse estrela, desnormalizado), aqui o modelo é
**normalizado para escrita concorrente**: cada fato é gravado uma vez e as
leituras do console usam joins por chave composta.
