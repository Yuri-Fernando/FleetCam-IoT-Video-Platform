# Revisão de DDL — FleetCam V1..V11

> Revisão feita sobre o catálogo real (laboratório) e sobre as consultas do código
> Kotlin. Migrações históricas **não** foram alteradas; correções viram a
> **proposta V12** ([`proposals/`](proposals/)), testada com forward + rollback em
> banco descartável e **não** aplicada pelo app.

## Pontos fortes

- `tenant_id` em todas as PKs/FKs de tabelas por dispositivo (isolamento garantido pelo banco).
- Idempotência declarativa (UNIQUE de 5 colunas em `ingest_events`; UNIQUE de offset em `dead_letters`).
- Índices parciais alinhados às consultas reais (`outbox_pending`, `evidence_pending_requested`).
- V10 adiciona coluna NOT NULL **com default constante** (sem reescrita de tabela no PG ≥ 11).
- V9 é um backfill defensivo: `pg_input_is_valid` evita que um payload não-JSON derrube a migração (verificado no laboratório).

## Achados

| ID | Achado | Evidência | Risco | Proposta (V12) |
|---|---|---|---|---|
| R1 | `incidents.status`, `evidence.status`, `dead_letters.status` são `text` sem CHECK; o domínio só existe no Kotlin | `descriptions.json`; código usa OPEN/CLOSED, PENDING/AVAILABLE/FAILED/EXPIRED, OPEN/REDRIVEN | valor inválido gravado por script/bug some das consultas `status='OPEN'` | CHECKs `ck_<tabela>_status` `NOT VALID` + `VALIDATE` |
| R2 | `incident_timeline.incident_id` é FK **sem índice** | catálogo: nenhum índice em `incident_timeline` além da PK | `GET /incidents/{id}` faz scan da timeline inteira; `DELETE` em `incidents` varre a timeline | `incident_timeline_incident (incident_id, occurred_at)` |
| R3 | `state_transitions` é a única tabela com `tenant_id`+`device_id` **sem FK** e sem índice | catálogo | transição órfã/cross-tenant possível; consulta por dispositivo sem índice | FK composta `NOT VALID` + `VALIDATE`; índice `(tenant_id, device_id, changed_at DESC)` |
| R4 | V11 faz `UPDATE evidence SET upload_requested_at = created_at` em todas as linhas | migração V11 | reescreve a tabela inteira numa transação (bloqueia escrita em `evidence` durante o backfill); aceitável no laboratório, arriscado em tabela grande | registrado: em produção, backfill em lotes fora da migração |
| R5 | `evidence.object_key` UNIQUE global, não por tenant | V3 | colisão entre tenants só se a chave não incluir o tenant; o app gera `"$tenant/staging/$id"` e `"$tenant/verified/$id"` (`Evidence.kt`), com UUID | aceito: documentado |
| R6 | `dead_letters.tenant_id` anulável | V8/V9 | consultas por tenant não veem DLTs não atribuídas | aceito por desenho (mensagem ilegível não tem tenant) |
| R7 | `audit_log` e `incident_timeline.event_id` sem FK | V2/V4 | — | aceito: logs append-only não devem falhar por referência ausente |

## Resultado do laboratório para a proposta V12

| Verificação | Resultado |
|---|---|
| aplica sobre V11 | ✅ 6 mudanças estruturais |
| constraints validadas (`convalidated`) | ✅ |
| `UPDATE incidents SET status='ABERTO'` rejeitado | ✅ `CheckViolation` |
| reaplicação (idempotência) | ✅ nenhuma mudança |
| rollback volta exatamente ao snapshot V11 | ✅ |

Para virar migração real: confirmar que não existe V12, mover para
`db/migration/V12__integrity.sql`, rodar `gradle test` + checks de integração.
