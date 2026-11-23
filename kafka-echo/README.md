# kafka-echo (v3)

Spring Boot 4 (Java 25) + Kafka. O endpoint `POST /api/echo` publica a mensagem num tópico e só responde
(via `DeferredResult`) quando o evento volta pelo listener **da mesma instância** que recebeu a requisição.
Foi pensado para rodar em várias instâncias ao mesmo tempo, atrás de um BFF que chama o endpoint de forma
síncrona.

## Novidades da v3 (resiliência sob stress)

1. **Publicação fora das threads do Tomcat, em pool limitado (bulkhead)** e **timeouts do producer** reduzidos.
   Com o broker lento ou fora do ar, o `send` não trava mais a aplicação inteira.
2. **Eventos expirados são descartados** antes de qualquer processamento (e antes do `CONSUMER_DELAY`),
   o que evita a "espiral de backlog".
3. **Readiness, liveness e encerramento gracioso** (dreno das requisições pendentes).

Da v2 continuam: `listener.concurrency` (padrão 3), listener em lote com threads virtuais, rebalance
cooperativo e parâmetros na inicialização.

## Fluxo

1. `POST /api/echo` com `{"message":"olá"}`: o controller gera um `correlationId` e registra o `DeferredResult`
   em `PendingRequestRegistry` (`ConcurrentHashMap`).
2. `EchoProducerService` enfileira a publicação em um pool próprio (`echo-publisher-N`), que envia ao tópico
   `echo-events` com os headers `x-correlation-id` e `x-service-instance-id` e a mensagem como payload.
3. Todas as instâncias consomem o tópico (cada uma com `group.id = echo-consumer-<instance-id>`).
4. `EchoEventListener` recebe o lote, descarta os eventos de outras instâncias e os sem requisição pendente e,
   nos demais, completa o `DeferredResult`. A resposta `{"message":"olá"}` volta ao cliente.

## Hierarquia de timeouts

Cada camada interna deve expirar **antes** da externa:

| Camada | Parâmetro | Padrão |
|---|---|---|
| Producer: bloqueio no `send` | `PRODUCER_MAX_BLOCK_MS` | 1500 ms |
| Producer: resposta do broker | `PRODUCER_REQUEST_TIMEOUT_MS` | 3000 ms |
| Producer: entrega total (com retries) | `PRODUCER_DELIVERY_TIMEOUT_MS` | 8000 ms |
| Endpoint (responde 504) | `REQUEST_TIMEOUT` | 10 s |
| BFF | (cliente) | 15 s |
| Proxy / ingress / mesh entre BFF e serviço | (infra) | deve ser maior que o do BFF |

Regras: `PRODUCER_DELIVERY_TIMEOUT_MS >= PRODUCER_REQUEST_TIMEOUT_MS` (exigência do Kafka) e
`PRODUCER_DELIVERY_TIMEOUT_MS <= REQUEST_TIMEOUT`, para que um evento nunca seja publicado depois de a
requisição já ter expirado.

## Códigos de resposta

| Status | Quando |
|---|---|
| 200 | resposta recebida |
| 400 | mensagem em branco |
| 502 | falha ao publicar no Kafka (broker indisponível, `max.block.ms` estourado, etc.) |
| 503 | fila de publicação cheia (sobrecarga) ou instância em encerramento |
| 504 | sem resposta em `REQUEST_TIMEOUT`, ou a publicação esperou na fila além desse prazo |

## Paralelismo do consumer

| Nível | Parâmetro | Padrão | O que faz |
|---|---|---|---|
| Threads de consumer | `LISTENER_CONCURRENCY` | 3 | N consumers no mesmo group.id, dividindo as partições (teto = nº de partições) |
| Eventos por lote em paralelo | `LISTENER_PARALLELISM` | 50 | até N eventos do mesmo `poll()` ao mesmo tempo, em threads virtuais |
| Tamanho do lote | `MAX_POLL_RECORDS` | 50 | eventos por `poll()` |

O `CONSUMER_DELAY` (teste) é aplicado só aos eventos desta instância **com requisição pendente**, em paralelo
dentro do lote. Falha em um evento é logada e o evento é descartado, sem reentregar o lote. O ack mode é o
padrão (`BATCH`): o commit ocorre depois que todo o lote foi processado.

## Bulkhead de publicação

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `PUBLISHER_THREADS` | 8 | threads do pool de publicação |
| `PUBLISHER_QUEUE` | 200 | fila do pool; cheia = 503 imediato |

Uma tarefa que esperou na fila mais que `REQUEST_TIMEOUT` é descartada sem enviar (a requisição já expirou) e
resulta em 504. O `PRODUCER_DELAY` (teste) não bloqueia nenhuma thread.

## Readiness, liveness e encerramento

Endpoints (Actuator):

- `GET /actuator/health/liveness`: o processo está vivo.
- `GET /actuator/health/readiness`: pronta para receber tráfego. Só fica `UP` quando o listener Kafka está
  rodando **e todas as partições do tópico estão atribuídas** a esta instância. Enquanto isso não ocorre
  (subida, rebalance), responde 503 e o balanceador não envia tráfego, evitando os 504 de warm-up.
- `GET /actuator/health`: detalhes (partições atribuídas x esperadas).

Encerramento (SIGTERM / `docker stop` / Ctrl+C):

1. `GracefulDrain` (primeira fase a parar) muda a readiness para `REFUSING_TRAFFIC`; novas requisições recebem 503.
2. Aguarda as requisições pendentes terminarem, até `DRAIN_TIMEOUT` (12 s), com o listener Kafka e o servidor web
   ainda ativos, para que elas possam receber a resposta.
3. O listener Kafka para; o servidor web termina as respostas em andamento (`server.shutdown: graceful`).

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `DRAIN_TIMEOUT` | 12s | espera pelas requisições pendentes |
| `SHUTDOWN_PHASE_TIMEOUT` | 20s | limite de cada fase de encerramento do Spring |

### Exemplo para Kubernetes

```yaml
containers:
  - name: kafka-echo
    readinessProbe:
      httpGet: { path: /actuator/health/readiness, port: 8080 }
      periodSeconds: 5
      failureThreshold: 2
    livenessProbe:
      httpGet: { path: /actuator/health/liveness, port: 8080 }
      periodSeconds: 10
    lifecycle:
      preStop:
        exec: { command: ["sleep", "5"] }   # dá tempo de o Service retirar o pod dos endpoints
terminationGracePeriodSeconds: 45           # preStop (5) + dreno (12) + parada (alguns segundos) com folga
```

No Kubernetes, `HOSTNAME` (nome do pod) vira o id da instância. Não fixe `INSTANCE_ID` com o mesmo valor em
várias réplicas: elas dividiriam o mesmo `group.id` e eventos seriam lidos pela instância errada.

## Requisitos

- JDK 25 e Maven 3.9+
- Docker (para o Kafka local)

## 1. Subir o Kafka e o Kafka UI

```bash
docker compose up -d
```

- Kafka (KRaft, sem Zookeeper): `localhost:9092`
- Kafka UI: http://localhost:8090

O tópico `echo-events` é criado pela própria aplicação na subida (3 partições por padrão). Para parar:
`docker compose down` (acrescente `-v` para apagar também os dados).

> Se o tópico já existia com menos de 3 partições, ele não é alterado. Para recriá-lo, apague-o no Kafka UI
> ou use `docker compose down -v`.

## 2. Testes unitários

```bash
mvn test
```

## 3. Subir uma ou mais instâncias

Cada instância precisa de uma porta diferente (variável `PORT`, padrão 8080). Use um terminal por instância.
O id da instância é gerado sozinho (`kafka-echo-<6 caracteres>`) e aparece no log de subida.

**PowerShell**

```powershell
# terminal 1
$env:PORT=8081; mvn spring-boot:run

# terminal 2
$env:PORT=8082; mvn spring-boot:run
```

**bash (Linux/macOS)**

```bash
PORT=8081 mvn spring-boot:run     # terminal 1
PORT=8082 mvn spring-boot:run     # terminal 2
```

**cmd (Windows)**

```cmd
set PORT=8081 && mvn spring-boot:run
```

Ajustando parâmetros (PowerShell; em bash use `VAR=valor mvn ...`):

```powershell
$env:PORT=8081; $env:LISTENER_CONCURRENCY=3; $env:MAX_POLL_RECORDS=100; $env:LISTENER_PARALLELISM=100; mvn spring-boot:run
```

Ou por argumento da aplicação (no PowerShell, o conjunto inteiro entre aspas):

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8081 --spring.kafka.listener.concurrency=3 --spring.kafka.consumer.max-poll-records=100"
```

### Gerando o jar

```bash
mvn package -DskipTests
```

```powershell
$env:PORT=8081; java -jar target/kafka-echo-0.0.3-SNAPSHOT.jar
$env:PORT=8082; java -jar target/kafka-echo-0.0.3-SNAPSHOT.jar --spring.kafka.listener.concurrency=2
```

```bash
PORT=8081 java -jar target/kafka-echo-0.0.3-SNAPSHOT.jar
PORT=8082 java -jar target/kafka-echo-0.0.3-SNAPSHOT.jar --spring.kafka.listener.concurrency=2
```

Confira que a instância está pronta antes de enviar tráfego:

```powershell
Invoke-RestMethod http://localhost:8081/actuator/health/readiness
```

## 4. Chamar o endpoint

**PowerShell** (`curl` no PowerShell é alias de `Invoke-WebRequest`; use um dos abaixo)

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8081/api/echo -ContentType 'application/json' -Body '{"message":"olá"}'

curl.exe -s -X POST http://localhost:8082/api/echo -H "Content-Type: application/json" -d "{\"message\":\"olá\"}"
```

**bash**

```bash
curl -s -X POST localhost:8081/api/echo -H 'Content-Type: application/json' -d '{"message":"olá"}'
```

## Configuração (application.yaml / variáveis de ambiente)

| Variável | Padrão | Uso |
|---|---|---|
| `PORT` | 8080 | porta HTTP |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | brokers |
| `INSTANCE_ID` | (vazio) | força o id da instância |
| `LISTENER_CONCURRENCY` | 3 | threads de consumer por instância |
| `MAX_POLL_RECORDS` | 50 | eventos por `poll()` |
| `LISTENER_PARALLELISM` | 50 | eventos de um lote processados em paralelo |
| `TOPIC_PARTITIONS` | 3 | partições ao criar o tópico (só se o tópico ainda não existe) |
| `PUBLISHER_THREADS` | 8 | threads do pool de publicação |
| `PUBLISHER_QUEUE` | 200 | fila do pool de publicação |
| `PRODUCER_MAX_BLOCK_MS` | 1500 | `max.block.ms` do producer |
| `PRODUCER_REQUEST_TIMEOUT_MS` | 3000 | `request.timeout.ms` do producer |
| `PRODUCER_DELIVERY_TIMEOUT_MS` | 8000 | `delivery.timeout.ms` do producer |
| `REQUEST_TIMEOUT` | 10s | timeout do endpoint (504) |
| `DRAIN_TIMEOUT` | 12s | espera pelas requisições pendentes no encerramento |
| `SHUTDOWN_PHASE_TIMEOUT` | 20s | limite de cada fase de encerramento |
| `PRODUCER_DELAY` | 0ms | atraso não bloqueante antes de enfileirar a publicação (testes) |
| `CONSUMER_DELAY` | 0ms | `Thread.sleep` no listener, só nos eventos com requisição pendente (testes) |

Cada parâmetro também pode ser passado como argumento (`--spring.kafka.listener.concurrency=5`), que tem
precedência sobre o `application.yaml`. `LISTENER_CONCURRENCY` acima do número de partições não ajuda.

Id da instância: `INSTANCE_ID` → `HOSTNAME` (somente no Kubernetes, nome do pod) → `kafka-echo-<6 hex aleatórios>`.

No PowerShell, as variáveis definidas com `$env:` valem só para aquela janela e continuam definidas nela
(para limpar: `Remove-Item Env:CONSUMER_DELAY`).

## Cenários de stress para reproduzir (com o projeto `kafka-echo-jmeter`)

| Cenário | Como provocar | Esperado |
|---|---|---|
| Broker indisponível | `docker compose stop kafka` durante o teste | 502 em ~1,5 s (não trava); `/actuator/health/readiness` fica 503 |
| Sobrecarga de publicação | `PUBLISHER_THREADS=1 PUBLISHER_QUEUE=1` com carga alta | 503 imediatos |
| Consumer lento | `CONSUMER_DELAY=2s` com carga alta | latência cresce; após o fim da carga, o backlog esvazia rápido (eventos expirados são descartados) |
| Encerramento durante carga | Ctrl+C numa instância no meio do teste | pendentes concluem (até 12 s); novas requisições recebem 503 |
| Instância reiniciada | derrubar e subir uma instância | readiness só `UP` com as 3 partições atribuídas |

## Teste de carga

O projeto `kafka-echo-jmeter` dispara requisições concorrentes para as instâncias (ver o README dele).

## Observações

- `auto-offset-reset: latest` + `group.id` novo a cada subida: grupos de execuções antigas ficam órfãos no
  broker até expirarem.
- A readiness considera as partições atribuídas; a posição inicial (`latest`) é resolvida no primeiro poll
  após a atribuição. Com `periodSeconds` de alguns segundos na probe, isso já ocorreu quando o tráfego chega.
- Com `latest`, um rebalance antes do primeiro commit pode fazer a instância pular eventos publicados nessa
  janela; o efeito é um 504 pontual.
