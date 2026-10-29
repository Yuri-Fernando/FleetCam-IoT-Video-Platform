# Ameaças e limites do laboratório

## Ativos e fronteiras

O laboratório mantém eventos, estado de dispositivo e metadados em PostgreSQL; mensagens e falhas em Kafka; imagens em um bucket S3; e traces em Jaeger. O simulador ou adapter cruza a fronteira HTTP da API. O publicador atravessa a fronteira banco/Kafka, enquanto uploads e downloads usam URLs temporárias diretamente no armazenamento. O console acessa a API por proxy local. A instalação publicada por Compose destina-se somente à máquina local.

## Cenários

| Ameaça | Controle atual | Lacuna |
|---|---|---|
| Evento forjado | chave local vinculada a tenant, checagem de campos e vínculo com dispositivo | chave compartilhada não autentica dispositivo; precisa identidade por device, rotação, TLS e rate limit |
| Repetição ou alteração de evento | evento aceito persistido, identificador único, hash e comparação no consumidor | idempotência não prova identidade nem evita sobrecarga |
| Acesso entre clientes | chaves locais vinculam dois tenants, com consultas filtradas e testes de acesso cruzado | não há gestão corporativa de identidades, papéis ou isolamento validado em produção |
| Exposição de imagem | armazenamento autenticado e URL temporária; acesso só após confirmação e dentro da retenção | URL é um segredo; falta autorização granular, varredura e controle de upload antes de armazenar |
| Alteração do staging após confirmação | cópia verificada separada, tamanho e SHA-256 conferidos | assinatura de arquivo não prova autenticidade nem ausência de conteúdo malicioso |
| Mensagens problemáticas | retries, DLT persistida e redrive auditado; consumidor DLT retenta | uma falha persistente pode bloquear partição; autorização de redrive é somente chave compartilhada |
| Indisponibilidade de broker | evento e outbox no mesmo commit; republicação com efeito idempotente | publicador é de instância única; falta limite de backlog e proteção de recursos |
| Relógio do dispositivo incorreto | horários de origem e recebimento separados | ordenação entre boots usa relógio de origem e pode falhar; replay antigo pode parecer sinal recente |
| Snapshot aponta para caminho indevido | leitura automática só dentro da raiz configurada, após resolução de caminho | webhook local não é autenticado; diretório deve ser confiável e o ambiente não deve ser exposto |
| Vazamento de credencial ou URL | portas publicadas em loopback; dados gerados ignorados no Git | credenciais de demonstração são públicas por definição; não reutilizar, e não registrar URLs assinadas |

As medidas atuais são adequadas somente à demonstração local. Antes de qualquer uso externo, é necessário desenhar e testar autenticação individual, autorização por tenant e recurso, TLS, cotas, gestão de segredos, retenção verificável e políticas para dados pessoais.
