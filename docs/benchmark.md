# Linha de base observada

Execução em 28/10/2026, Windows, Python 3.10.8, Docker local, processador Intel Family 6 Model 158, com outra carga de CPU ativa. O cliente enviou 10 heartbeats sequencialmente para um dispositivo sintético. Foram aceitos e processados 10 de 10, com zero erros de cliente. A duração de envio foi 24,60 s e a reconciliação ocorreu em 30,79 s após o início.

Latência HTTP de aceite observada: p50 2.331 ms, p95 3.098 ms e p99 3.098 ms. Com somente dez amostras, p95 e p99 são o maior valor da amostra; não são estimativas estatisticamente robustas. Esse tempo inclui HTTP e transação de aceite, não mede a latência ponta a ponta até a projeção. Não foi medido lag Kafka, saturação, concorrência, uso de CPU/RAM do processo FleetCam nem desempenho em hardware isolado. Estes resultados não comprovam SLO ou capacidade industrial.

Repita com `py -3.10 simulator/benchmark.py --count 30`. O JSON detalhado é gerado em `simulator/output/benchmark.json`, ignorado pelo Git. Compare somente cenários com configuração e carga de máquina documentadas.

## Rajada concorrente adicional — 29/10/2026

Execução local com 30 heartbeats de um dispositivo sintético e quatro workers: 30 processados, zero erros, 16,92 s de envio e 18,99 s até a reconciliação. Latência HTTP p50 2.101 ms, p95 2.185 ms e p99 2.186 ms; tempo do aceite até o processamento p50 1.736 ms, p95 2.266 ms e p99 2.300 ms. A amostra é pequena e não mede lag Kafka nem define capacidade industrial. Reproduza com `py -3.10 simulator/benchmark.py --count 30 --workers 4`.

> **Correção em 29/10/2026:** as duas amostras acima foram medidas com o cliente apontando para `http://localhost:8080`. No Windows, `localhost` resolve primeiro para `::1`; como a porta está publicada só em `127.0.0.1`, cada conexão espera o fallback para IPv4 (~2 s). Esses números medem principalmente esse atraso do cliente, não a API. Desde então os clientes usam `127.0.0.1` por padrão (`--base` permite escolher).

## Antes/depois — 29/10/2026

Mesmo host (Windows, Python 3.10.8, Docker Desktop, Intel Family 6 Model 158), um único device sintético, sem isolar outras cargas. Cada linha é uma execução de `simulator/benchmark.py`; o JSON fica em `simulator/output/` (ignorado pelo Git). Percentis pelo método do posto mais próximo; com 10 amostras, p95 = p99 = maior valor. O lag é amostrado a cada 1 s a partir do gauge `fleetcam_kafka_consumer_lag`, que o servidor recalcula a cada 5 s (offset final − offset confirmado do grupo), então é uma medida grosseira.

### 1. Cliente: `localhost` × `127.0.0.1` (10 heartbeats, 1 worker)

| Base | Envio total | HTTP p50 | HTTP p95 | Aceite→processamento p50 |
|---|---:|---:|---:|---:|
| `http://localhost:8080` | 21,32 s | 2.088,6 ms | 2.405,1 ms | 866 ms |
| `http://127.0.0.1:8080` | 0,48 s | 37,2 ms | 83,2 ms | 916 ms |

Sonda direta em `/actuator/health`: cinco chamadas a `localhost` de 2.041 a 2.176 ms; a `127.0.0.1`, de 4,4 a 29,1 ms. O tempo aceite→processamento (~0,9 s) reflete o intervalo de 2 s do publicador do outbox.

### 2. Servidor: rajada de 500 heartbeats, 8 workers

| Variante do publicador | Execução | Envio | Reconciliação total | HTTP p50 / p95 / p99 (ms) | Aceite→processamento p50 / p95 / p99 (ms) | Lag máx. |
|---|---|---:|---:|---|---|---:|
| Original: envio síncrono, commit por linha | 1 | 34,88 s | 61,32 s | 579 / 826 / 1.026 | 29.928 / 32.205 / 32.383 | 233 |
| Drenar lotes cheios (hipótese 1) | 1 | 34,88 s | 63,45 s | 584 / 832 / 917 | 29.321 / 30.705 / 31.171 | 213 |
| Drenar lotes cheios (hipótese 1) | 2 | 35,09 s | 61,57 s | 578 / 871 / 945 | 29.413 / 32.550 / 32.946 | 225 |
| Envio assíncrono + commit por lote (ADR 003) | 1 | 24,98 s | 43,40 s | 382 / 576 / 709 | 20.121 / 22.217 / 22.330 | 370 |
| Envio assíncrono + commit por lote (ADR 003) | 2 | 23,87 s | 42,17 s | 369 / 551 / 650 | 19.637 / 21.314 / 21.398 | 401 |

Todas as execuções processaram 500 de 500 eventos com zero erros de cliente.

**Investigação.** A hipótese 1 (o limite de 50 linhas a cada 2 s restringia a vazão) foi refutada: drenar sem esperar o próximo ciclo não alterou o resultado. O intervalo entre marcações `delivered_at` consecutivas mostrou cerca de 69 ms por evento (411 de 499 intervalos entre 50 e 100 ms), com atraso do outbox p50 16,1 s e p95 25,1 s. Uma sonda no próprio contêiner do PostgreSQL mediu 50 `UPDATE` em autocommit em 2.741 ms (~55 ms por commit) e `pg_test_fsync` relatou 35,1 ms por operação `open_datasync`. A causa é o custo de fsync por commit no volume do Docker Desktop deste host, multiplicado por um commit por mensagem. Com o commit por lote, a reconciliação caiu cerca de 31% e o p50 aceite→processamento, cerca de 33%; o HTTP também melhorou, provavelmente por menor disputa de commits com a ingestão. O lag maior mostra que o gargalo passou para o consumidor, que ainda faz um commit por registro.

**Limites.** Um device só serializa a ingestão pelo lock da linha do device; vários devices teriam outro perfil. Não houve medição de CPU/RAM, de hardware isolado nem de várias instâncias. `synchronous_commit=off` reduziria o custo, mas quebraria a promessa de que HTTP 202 significa persistência local, e não foi adotado. Estes números não definem capacidade nem SLO.

Reproduza com `py -3.10 simulator/benchmark.py --count 500 --workers 8` e, para o artefato do cliente, `--count 10 --base http://localhost:8080`.
