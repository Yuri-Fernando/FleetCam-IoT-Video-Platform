# Indicadores e metas hipotéticas

Estas são propostas para o laboratório, não resultados ou garantias de produção. Use uma janela e volume suficientes antes de interpretar percentis ou disponibilidade.

- **Aceite HTTP:** fração de requisições de ingestão com resposta 2xx entre requisições elegíveis; rejeições de contrato 4xx devem ser analisadas separadamente. Uma resposta 202 confirma persistência, não processamento.
- **Latência HTTP:** p95/p99 dos histogramas de requisição da API; a medição do cliente também é registrada pelo benchmark. A latência inclui o caminho HTTP/DB de aceite.
- **Entrega:** razão entre eventos aceitos e projetados numa janela, reconciliando outbox pendente, eventos não processados e DLT. A consulta de backlog não substitui medição de perda ou lag Kafka.
- **Disponibilidade operacional de device:** proporção de dispositivos com heartbeat recente; não confundir indisponibilidade de backend, atraso Kafka e falta de sinal da câmera.

Metas iniciais a avaliar em carga controlada: 99% dos eventos aceitos projetados em até 60 segundos após aceite; 99% de disponibilidade da API no laboratório; p95 de aceite abaixo de 2 segundos. O teste registrado em `benchmark.md` é pequeno e não prova atendimento. Alertas Prometheus de indisponibilidade de API, outbox e DLT são hipóteses operacionais; não há roteamento de notificação configurado. O lag do consumidor Kafka é instrumentado (`fleetcam_kafka_consumer_lag`) e tem alerta; burn rate ainda não é instrumentado.
