# Investigação operacional local

1. Consulte `/api/v1/ops/overview` com a chave de laboratório. `outbox_pending` mede mensagens aceitas ainda sem confirmação do broker; `unprocessed` mede eventos ainda sem projeção. Nenhum deles significa, por si só, câmera offline.
2. Compare `last_seen_at` com o horário local e o histórico em `/api/v1/devices/{id}/timeline`. O horário de recebimento é do servidor; `occurred_at` vem do relógio do dispositivo e pode estar errado.
3. Consulte `/api/v1/incidents` para sinais de SD/captura, e `sql/investigation.sql` para backlog e coortes de firmware. Uma correlação não prova causa raiz.
4. Verifique `docker compose logs api kafka postgres objectstore`, o Prometheus em `http://localhost:9090` e Grafana em `http://localhost:3000` (credenciais descartáveis `admin` / `fleetcam_local`). Não registre credenciais nem payloads sensíveis em relatórios.
5. Após corrigir a causa, verifique `/api/v1/lab/dlt` e use `POST /api/v1/lab/dlt/{id}/redrive` somente no laboratório. O redrive é registrado em `audit_log`; eventos sem registro aceito ou já processados não são reprocessados por esse caminho.
6. Para indisponibilidade de evidência, confirme se o objeto foi efetivamente enviado e repita a confirmação. PENDING não é um arquivo disponível. O endpoint de acesso rejeita objetos não confirmados e expirados. Uploads não confirmados em 900 s viram FAILED com `failure_reason` UPLOAD_MISSING ou UPLOAD_UNCONFIRMED (ver `GET /api/v1/evidence`); o dispositivo deve pedir novo upload com a mesma chave de idempotência. `fleetcam_evidence_pending` alto por muito tempo indica storage indisponível para o job de reconciliação.
7. Para atraso de painel, compare `fleetcam_outbox_pending` (ainda não publicado) com `fleetcam_kafka_consumer_lag` (publicado e não consumido). São gargalos diferentes; nenhum deles indica câmera offline.

## RCA sintético: broker indisponível

Hipótese: eventos aceitos acumulam no outbox quando Kafka está parado. `py -3.10 simulator/fault_check.py` para apenas o serviço Kafka do Compose deste projeto, cria um heartbeat, verifica sua persistência, reinicia o broker e aguarda a projeção. A falha de conectividade da câmera não é inferida a partir da indisponibilidade do broker. O script também força a republicação de um evento já processado e verifica o efeito único.

## RCA sintético: queda abrupta do processo

Hipótese: um SIGKILL entre a projeção e o commit do offset faz o Kafka reentregar mensagens já projetadas. `py -3.10 simulator/crash_check.py` cria um backlog de 300 heartbeats com o broker parado, religa o broker, mata a API com SIGKILL durante o processamento, reinicia e verifica 300 projeções únicas, sequência final 300 e lag 0. Sinal observado: `fleetcam_projector_redeliveries_total` > 0 após o reinício, sem efeito duplicado no banco.

## RCA medido: atraso de processamento numa rajada

Sintoma: rajada de 500 eventos levava cerca de 30 s (p50) do aceite ao processamento. Hipótese 1, limite de lote do publicador, foi refutada por experimento. Os intervalos entre `delivered_at` (~69 ms) e uma sonda de commits no PostgreSQL (~55 ms) apontaram o fsync por commit. Correção: commit por lote no outbox (ADR 003), com p50 de cerca de 20 s. Próximo gargalo: commit por registro no consumidor. Detalhes em [benchmark](benchmark.md).

## Resumo para suporte

"Houve atraso na distribuição de sinais ao painel enquanto o serviço de mensagens estava indisponível. Os sinais aceitos ficaram registrados para reenvio. Após a recuperação, o processamento retomou. A indisponibilidade do serviço de mensagens não comprova defeito nas câmeras."

## Segurança e limites

O ambiente é uma instalação de laboratório, com um tenant fixo e chave compartilhada. Identidade por dispositivo, TLS, controle por papéis, proteção de tamanho de upload antes do armazenamento, autenticação corporativa, isolamento multi-tenant real e varredura de arquivos não estão implementados. As portas são vinculadas a loopback; não exponha o Compose à internet nem use imagens de pessoas. Presigned URLs são credenciais de curta duração e devem ser tratadas como segredo. A assinatura e o hash não verificam malware nem comprovam a origem de uma imagem.
