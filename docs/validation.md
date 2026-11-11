# Validação local — 29/10/2026

A fonte validada foi a pasta do projeto em G:, com Python 3.10 e Docker Desktop. Não se trata de validação de produção nem de CI remoto.

- Build Kotlin/Spring e build do console React concluíram com sucesso. Uma imagem intermediária tinha um JAR de zero bytes após interrupção; foi reconstruída sem cache e o novo JAR foi conferido antes da inicialização.
- Testes Python do simulador e do spool VisionBrain passaram. A importação do spool legado preserva o arquivo de entrada e deduplica registro repetido.
- Ingestão, duplicata, conflito de identidade e heartbeat atrasado passaram. A parada do Kafka preservou o evento aceito no outbox, que foi processado depois da retomada; a publicação repetida não gerou outro efeito.
- Falha de projeção controlada levou evento à DLT; após remoção da falha, redrive processou o evento e gerou registro de auditoria.
- Uma falha sintética de SD abriu e fechou incidente. Um teste isolado verificou OFFLINE e retorno a ONLINE após dois heartbeats. Em um cenário temporizado de 75 segundos, o dispositivo ficou OFFLINE com duas mensagens no spool e voltou a ONLINE após o replay, com spool vazio.
- O teste de evidência validou upload assinado, SHA-256, leitura, cópia verificada imutável perante novo upload no staging e recusa de hash incorreto. Um snapshot sintético também foi anexado com a ferramenta de linha de comando.
- O VisionBrain instalado no projeto vizinho analisou um frame preto sintético; o evento nativo atravessou o adapter e foi recebido na API. Isso é análise de qualidade de captura, não inferência validada de segurança viária.
- No navegador, o console exibiu um evento `hard_brake_simulated` marcado como SIMULADO, um evento `VISIONBRAIN_ANALYSIS`, uma imagem confirmada e a timeline de um incidente com abertura e recuperação.
- As quatro métricas da API foram comparadas no mesmo momento às contagens SQL: outbox 0, eventos não processados 0, DLT aberta 1 e incidentes abertos 43. O Prometheus coletou o indicador do outbox. A versão atualizada do painel foi verificada posteriormente pela API do Grafana; não houve nova inspeção visual nessa etapa.

Não foram comprovados: crash abrupto do consumidor antes de commit, autenticação por dispositivo e isolamento multi-tenant de produção, vídeo ao vivo, benchmark de carga representativo, SLO ou execução remota de CI. Consulte [decisões e pendências](decisions.md) e [benchmark](benchmark.md).

- Após a validação original, uma execução do VisionBrain gerou snapshot sintético; o adapter enviou e confirmou a evidência automaticamente. A repetição após perda do reconhecimento local retornou a mesma evidência. Um teste unitário também verificou rejeição de caminho fora da pasta configurada.

- Testcontainers com PostgreSQL 16 e Kafka 3.8.1 confirmou reentrega após o consumidor fechar sem commit e uma única projeção. Este cenário reproduz o offset não confirmado, mas não mata o processo abruptamente. O Compose do laboratório utiliza Kafka 3.9.
- A API passou nos testes de rejeição de valores inválidos e consulta de dispositivo além das primeiras 500 linhas.

- Um teste com `traceparent` confirmou o mesmo trace no HTTP, no outbox, no recebimento Kafka e no Jaeger. A timeline expõe o traceparent.

- O teste `reboot_check.py` confirmou que eventos de um boot encerrado são registrados sem sobrescrever o boot atual. O Prometheus carregou três regras de alerta, aprovadas pelo `promtool`; histogramas HTTP foram expostos. O painel provisionado do Grafana retornou os sete painéis via API.
- `scripts/demo.ps1 -OfflineSeconds 0` passou como teste da sequência; a janela de 75 segundos foi verificada em execução separada.

- Dois testes unitários da política de SD passaram, assim como os testes integrados de incidente, reboot e trace com os limiares configuráveis. A política considera aumentos sustentados no contador e leituras estáveis de recuperação; ausência do dado permanece UNKNOWN.

- A demonstração completa com 75 segundos offline passou, incluindo recuperação de SD, evidência, tracing e indisponibilidade Kafka. O teste de 20 dispositivos sintéticos ONLINE também passou.

- Duas chaves de laboratório foram testadas para acesso cruzado a dispositivos, eventos e evidências confirmadas; o tenant B não acessou os recursos do tenant A nem a administração local. Essa verificação não estabelece segurança para produção.

- O teste de ciclo de vida confirmou manutenção, desativação, bloqueio de novos heartbeats e supressão de alertas de conectividade.
- O teste de replay após desativação interrompeu o Kafka, aceitou um heartbeat, desativou o dispositivo e confirmou que, após a recuperação, o evento foi marcado como processado sem projetar estado nem abrir incidentes.
- Após isolamento de tenants e ciclo de vida, o teste Testcontainers de reentrega foi repetido: 1 teste, nenhuma falha, erro ou omissão. Continua sendo fechamento sem commit, não crash abrupto.

## Revisão de 29/10/2026 (fechamento dos gates M1/M3/M5)

- `simulator/evidence_reconcile_check.py`: um ticket recente permanece PENDING numa janela de 600 s; um upload nunca enviado vira FAILED/UPLOAD_MISSING; um upload parcial sem confirmação vira FAILED/UPLOAD_UNCONFIRMED; ambos recusam acesso e confirmação tardia (409); a repetição com a mesma chave de idempotência emite novo ticket, conclui e libera leitura; o tenant B recebe 403 no endpoint de laboratório.
- `simulator/crash_check.py`: com o Kafka parado, 300 heartbeats foram aceitos; após religar o broker, a API recebeu SIGKILL com o backlog em andamento (execução 1: 9 processados e 10 publicados no momento do kill; execução 2, após o ADR 003: 1 processado e 300 publicados). Após o reinício, 300 de 300 foram projetados uma vez, `sequence_number` final = 300, uma única transição de estado, nenhum incidente de SD/captura e lag de volta a 0. Nas duas execuções o contador `fleetcam_projector_redeliveries_total` registrou 1 reentrega reconhecida. É um crash real do processo no Compose, não no Testcontainers.
- Suíte completa repetida após o ADR 003: 17 de 17 checks de integração passaram; `scripts/demo.ps1 -OfflineSeconds 0` passou; `gradle test bootJar` em contêiner `gradle:8.10.2-jdk17` passou com 3 testes (RedeliveryTest com Testcontainers e 2 de StoragePolicy), sem falhas.
- `promtool check rules`: 5 regras. O Prometheus coletou `fleetcam_kafka_consumer_lag`, `fleetcam_projector_redeliveries_total` e `fleetcam_evidence_pending`; a API do Grafana retornou 10 painéis no painel provisionado. Não houve inspeção visual nesta revisão.
- Ainda não comprovados: benchmark com vários devices/instâncias, SLO.
- Primeira execução do GitHub Actions (29/10/2026): job `build` (Gradle `test bootJar`, com Testcontainers, e testes unitários Python) passou; job `integration` falhou no `tenant_check` com 503 porque o SeaweedFS ainda inicializava. Corrigido com `simulator/wait_ready.py` (upload S3 real antes dos checks), verificado localmente após reiniciar o objectstore (28 s de espera, `tenant_check` passou em seguida).
- Segunda execução do GitHub Actions após a correção: jobs `build` e `integration` passaram, incluindo os 17 checks (com `fault_check`, `decommission_replay_check` e `crash_check`) num runner Ubuntu: https://github.com/Yuri-Fernando/FleetCam-IoT-Video-Platform/actions/runs/40957044096

## Validação local — 10/11/2026 (v0.3, modelagem de dados)

- `scripts/data-model/migration_lab.py`: PostgreSQL 16.15 + `flyway/flyway:10.10.0` em rede Docker isolada; V1→V11 aplicadas uma a uma, com dados semeados após V1, V7 e V10. 23 de 23 verificações passaram: histórico Flyway com 11 sucessos; dados de V1 preservados; backfills de V10 (`PROVISIONED`), V7 (defaults 0), V9 (só a dead letter cujo payload casa com evento aceito recebe tenant; JSON inválido não quebra) e V11 (`upload_requested_at = created_at`); FK composta rejeita referência cross-tenant; UNIQUE de sequência rejeita replay; UNIQUE parcial de incidentes (rejeita 2º OPEN, `ON CONFLICT DO NOTHING` sem efeito, aceita outro tipo, reabre após fechar); CHECKs de `byte_size` e `lifecycle_state`; o polling real do outbox usa `outbox_pending` (16 KiB contra 152 KiB de índice comum, 20.006 linhas/6 pendentes); proposta V12 aplica, valida, rejeita status inválido, é idempotente e o rollback volta exatamente ao snapshot V11; dicionário cobre 100 de 100 colunas. Containers removidos ao final.
- `python -m unittest discover -s scripts/data-model`: 8 testes OK; `simulator` e `adapters`: OK.
- Não reexecutados nesta data: testes Kotlin e os 17 checks de integração (nenhum código Kotlin, migração ou Compose foi alterado).
