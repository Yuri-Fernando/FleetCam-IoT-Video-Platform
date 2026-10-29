# Decisões e backlog

## ADR 001: monólito modular e JDBC

O backend usa Spring Boot/Kotlin, JDBC e migrações Flyway. PostgreSQL é a fonte operacional e Kafka transporta mensagens pequenas, particionadas por tenant e device. A inserção do evento e do outbox compartilha uma transação; o publicador marca a entrega após confirmação do broker. Uma queda depois da publicação e antes da marcação pode produzir duplicatas, por isso o processamento marca o evento na mesma transação da projeção. Não há garantia exactly-once entre serviços.

O primeiro corte suporta uma única instância do publicador. Múltiplas instâncias exigem coordenação/claim do outbox e testes de concorrência. Um boot já encerrado não regride a projeção. Para boots anteriormente desconhecidos, a ordenação depende do relógio reportado pelo dispositivo e não é suficiente contra clock skew; relógio confiável ou uma política explícita de reconciliação é necessária.

Falhas no consumidor de ingestão são retentadas e enviadas à DLT Kafka; o registro é projetado em PostgreSQL. O consumidor da DLT retenta sem limite se não puder persistir, podendo bloquear sua partição. Redrive de eventos aceitos e ainda não processados é explícito e auditado. O acesso usa duas chaves compartilhadas vinculadas a tenants de laboratório, sem papéis de produção.

## ADR 002: reconciliação de upload de evidência interrompido

Um ticket de upload tem URL assinada válida por 300 s. Se o cliente nunca envia o objeto, ou envia parcialmente e não confirma, a linha permaneceria PENDING para sempre. Decisão: cada emissão de ticket registra `upload_requested_at`; um job a cada 60 s marca como FAILED as evidências PENDING mais antigas que `fleetcam.evidence.pending-timeout-seconds` (padrão 900 s), com `failure_reason` UPLOAD_MISSING (objeto ausente) ou UPLOAD_UNCONFIRMED (objeto presente, não confirmado; o staging é removido). Uma confirmação tardia de upload abandonado é recusada com 409; repetir o pedido com a mesma chave de idempotência emite novo ticket e volta a PENDING. Se o armazenamento estiver indisponível, a linha continua PENDING e é tentada de novo, sem afirmar falha que não foi observada. A atualização usa `WHERE status='PENDING'`, que serializa com o `FOR UPDATE` da confirmação. O endpoint `POST /api/v1/lab/evidence/reconcile` existe somente para teste do laboratório (tenant de demonstração). Verificado por `simulator/evidence_reconcile_check.py`.

## ADR 003: commit do outbox por lote

Medição em 29/10/2026 mostrou que cada commit do PostgreSQL custa cerca de 55 ms neste host (fsync do volume do Docker Desktop; ver [benchmark](benchmark.md)). O publicador fazia envio síncrono e um commit por linha, cerca de 69 ms por evento. Decisão: enviar o lote de até 50 registros de forma assíncrona, aguardar as confirmações e marcar `delivered_at` das linhas confirmadas numa única transação; enquanto o lote vier cheio, drenar dentro de um orçamento de 1,5 s para não bloquear o agendador compartilhado. Consequência: uma queda entre a confirmação do broker e o commit republica o lote inteiro, e não só uma mensagem; o consumidor já reconhece a reentrega (`fleetcam_projector_redeliveries_total`). Se uma mensagem falha no meio do lote, as seguintes confirmadas são marcadas e a falha é reenviada depois, possivelmente fora de ordem; a projeção já ordena por `boot_id`/`sequence_number`. O drenar sozinho foi testado antes e não mudou o resultado; o ganho veio do commit por lote. O consumidor ainda faz um commit por registro e passou a ser o gargalo.

## Marcos

- M0: contrato, decisões, diagrama e modelo de ameaças do laboratório registrados; faltam aprofundamento e revisão independente para qualquer uso externo.
- M1: registro, ingestão, outbox e projeção; teste local com 20 devices e indisponibilidade Kafka passou. Testcontainers comprovou reentrega sem commit; `crash_check.py` mata o processo da API com SIGKILL no meio de um backlog de 300 eventos e confirma projeção única após o reinício. A mesma suíte passa no GitHub Actions.
- M2: cenários, timeline, DLT/redrive e incidentes básicos; sessões de boot, replay de boot encerrado e histerese configurável de SD foram testados; faltam políticas completas para as demais dimensões e ground truth completo.
- M3: fluxo S3, integridade, retenção, reconciliação de upload interrompido (ADR 002) e Prometheus/Grafana implementados e verificados; tracing HTTP–outbox–Kafka validado no Jaeger.
- M4: adapter VisionBrain e console React básicos implementados; o envio automático do snapshot nativo foi implementado e testado; faltam outras jornadas completas previstas no escopo.
- M5: runbook, RCA de laboratório, benchmark de rajada com p50/p95/p99 e lag do consumidor medidos, e comparação antes/depois (ADR 003) registrados; a demonstração completa com 75 segundos offline foi validada. O benchmark continua sendo de um único host e um único device.
- M6: integrações físicas e SaaS são opcionais e não implementadas.

## Limites de segurança

Duas chaves compartilhadas vinculadas a tenants distintos permitem testes locais de isolamento; são exclusivas de laboratório e não substituem autenticação corporativa ou individual por dispositivo. O Compose publica a API somente em loopback, mas não fornece TLS, gestão de identidades, limites de requisição, autorização granular, segredos seguros ou isolamento multi-tenant validado para produção. Não utilizar dados pessoais ou expor esta configuração na internet.

O ciclo de vida distingue PROVISIONED, ACTIVE, MAINTENANCE e DECOMMISSIONED. A desativação é definitiva neste laboratório, bloqueia novas ingestões e serializa com a projeção para evitar que eventos atrasados reativem o estado.
