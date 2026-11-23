# kafka-echo

Spring Boot 4 (Java 25) + Kafka. O endpoint `POST /api/echo` publica a mensagem num tópico e só responde
(via `DeferredResult`) quando o evento volta pelo listener **da mesma instância** que recebeu a requisição.
Foi pensado para rodar em várias instâncias ao mesmo tempo.

## Fluxo

1. `POST /api/echo` com `{"message":"olá"}`: o controller gera um `correlationId` e registra o `DeferredResult`
   em `PendingRequestRegistry` (`ConcurrentHashMap`).
2. `EchoProducerService` publica em `echo-events` com os headers `x-correlation-id` e
   `x-service-instance-id` (id único da instância) e a mensagem como payload.
3. Todas as instâncias consomem o tópico (cada uma com `group.id = echo-consumer-<instance-id>`).
4. `EchoEventListener` ignora eventos cujo `x-service-instance-id` não é o seu; nos demais, completa o
   `DeferredResult` do `x-correlation-id` e a resposta `{"message":"olá"}` volta ao cliente.

## Requisitos

- JDK 25 e Maven 3.9+
- Docker (para o Kafka local)

## 1. Subir o Kafka e o Kafka UI

```bash
docker compose up -d
```

- Kafka (KRaft, sem Zookeeper): `localhost:9092`
- Kafka UI: http://localhost:8090 (tópico `echo-events`, mensagens com headers e um consumer group por instância)

O tópico `echo-events` é criado pela própria aplicação na subida. Para parar: `docker compose down`
(acrescente `-v` para apagar também os dados).

## 2. Testes unitários

```bash
mvn test
```

## 3. Subir uma ou mais instâncias

Cada instância precisa de uma porta diferente (variável `PORT`, padrão 8080). Use um terminal por instância.
O id da instância é gerado sozinho (`kafka-echo-<6 caracteres>`) e aparece no log de subida, junto com o `group.id`.

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

Alternativa sem variável de ambiente (no PowerShell o argumento inteiro precisa estar entre aspas):

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8081"
```

### Gerando o jar (evita recompilar a cada instância)

```bash
mvn package -DskipTests
```

```powershell
$env:PORT=8081; java -jar target/kafka-echo-0.0.1-SNAPSHOT.jar   # terminal 1
$env:PORT=8082; java -jar target/kafka-echo-0.0.1-SNAPSHOT.jar   # terminal 2
```

```bash
PORT=8081 java -jar target/kafka-echo-0.0.1-SNAPSHOT.jar
PORT=8082 java -jar target/kafka-echo-0.0.1-SNAPSHOT.jar
```

Espere alguns segundos após a subida: o consumer usa `auto-offset-reset: latest` com um grupo novo, então
requisições feitas logo no início podem dar 504 até ele receber as partições.

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

Cada chamada é respondida pela instância que a recebeu.

## Configuração (application.yaml / variáveis de ambiente)

| Variável | Padrão | Uso |
|---|---|---|
| `PORT` | 8080 | porta HTTP |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | brokers |
| `INSTANCE_ID` | (vazio) | força o id da instância |
| `REQUEST_TIMEOUT` | 10s | timeout do endpoint (504) |
| `PRODUCER_DELAY` | 0ms | atraso não bloqueante antes de publicar (testes) |
| `CONSUMER_DELAY` | 0ms | `Thread.sleep` no listener, só nos eventos desta instância (testes) |

Exemplo (PowerShell) para provocar 504: `$env:CONSUMER_DELAY="15s"; $env:PORT=8081; mvn spring-boot:run`.

O consumer lê no máximo 50 eventos por `poll()` (`max-poll-records: 50`), para que um `CONSUMER_DELAY` alto não
estoure o `max.poll.interval.ms` (5 min) e cause rebalance. O ack mode do listener continua o padrão (`BATCH`).

Id da instância: `INSTANCE_ID` → `HOSTNAME` (somente no Kubernetes, nome do pod) → `kafka-echo-<6 hex aleatórios>`.

No PowerShell, as variáveis definidas com `$env:` valem só para aquela janela e continuam definidas nela
(para limpar: `Remove-Item Env:CONSUMER_DELAY`).

## Teste de carga

O projeto `kafka-echo-jmeter` dispara requisições concorrentes para as instâncias (ver o README dele).

## Observações

- `auto-offset-reset: latest` + `group.id` novo a cada subida: grupos de execuções antigas ficam órfãos no
  broker até expirarem.
- Respostas de erro: 400 (mensagem em branco), 502 (falha ao publicar no Kafka), 504 (sem resposta no prazo).
