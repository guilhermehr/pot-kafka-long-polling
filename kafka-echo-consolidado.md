# kafka-echo: consolidado de opções exploradas e histórico de versões

Documento de referência com tudo o que foi discutido e construído desde a primeira versão: requisitos, decisões,
alternativas analisadas, resumo de alterações por versão e pendências.

> **Aviso:** o código, os testes e os comandos de todas as versões foram escritos sem acesso a Maven, JMeter,
> Docker ou rede no ambiente de geração. **Nada foi compilado nem executado.** Rode `mvn test` em cada projeto
> antes de usar. As afirmações sobre padrões do Kafka e do Spring Kafka (ack mode, `max.poll.records`, assignors)
> vêm do conhecimento dos padrões, sem verificação em execução.

---

## 1. Requisitos originais

- Projeto Java 25 com Spring Boot 4 e testes unitários.
- Endpoint REST que recebe uma string e retorna uma string, com `DeferredResult`.
- O controller repassa a string a um service (producer) que publica em um tópico Kafka, com **headers** (id único
  da requisição e id/artifactId da instância) e a string no **payload**.
- No mesmo projeto, um consumer do mesmo tópico. **Cada instância com `group.id` distinto**, consumindo todos os
  eventos. O listener filtra pelo header do id da instância, localiza a requisição pelo id e devolve o payload
  ao chamador.
- `.yaml` com configurações de Kafka, tópico e **atrasos de teste** separados para producer e consumer.
- Várias instâncias, cada uma com id único (nomenclatura estilo Kubernetes ou solução mais simples).

### Decisões tomadas na confirmação do plano

| Ponto | Decisão |
|---|---|
| Build | Maven |
| Endpoint | `POST` com corpo JSON (`{"message":"..."}`), resposta JSON |
| Offsets e grupos | Aceito: `group.id` distinto por instância, `auto-offset-reset: latest`, grupos órfãos ao reiniciar |
| Pacote base | `com.exemplo.kafkaecho`, artifactId `kafka-echo` |
| Extras (docker-compose, EmbeddedKafka) | Sem extras na primeira entrega (compose foi pedido depois) |
| Controle em memória | `ConcurrentHashMap` (`PendingRequestRegistry`) |

---

## 2. Resumo de alterações por versão

### v1: `kafka-echo` 0.0.1-SNAPSHOT

**Entrega inicial**

- Java 25, Spring Boot 4.1.0, Maven. Starters `webmvc`, `kafka`, `validation`.
- `POST /api/echo` com `DeferredResult<EchoResponse>`; timeout de 10 s (`REQUEST_TIMEOUT`) responde 504;
  falha ao publicar responde 502; mensagem em branco responde 400.
- `PendingRequestRegistry` com `ConcurrentHashMap<String, DeferredResult<EchoResponse>>`. O `remove` atômico
  evita conclusão dupla entre listener e timeout. A requisição é registrada **antes** de publicar.
- `EchoProducerService`: publica com os headers `x-correlation-id` e `x-service-instance-id`, payload com a
  string. Delay de teste do producer não bloqueante (`CompletableFuture.delayedExecutor`).
- `EchoEventListener` (um evento por vez): `@KafkaListener` com `groupId` por SpEL (`echo-consumer-<id>`), filtro
  por instância, conclusão por `correlationId`. Delay de teste do consumer com `Thread.sleep`.
- Id da instância: `INSTANCE_ID` explícito, depois `HOSTNAME` **somente no Kubernetes**, depois
  `kafka-echo-<6 hex aleatórios>`.
- `application.yaml` com variáveis de ambiente para porta, brokers, timeout e delays.
- Testes unitários: registry (incluindo concorrência), resolvedor de id, producer, listener e controller
  (200, 504 por timeout simulado no `AsyncContext`, 502, 400).

**Ajustes feitos no próprio v1 antes da v2**

- Delay do consumer movido para **depois do filtro por instância** (só eventos da própria instância esperam).
- `max-poll-records: 50` no `application.yaml`.
- `docker-compose.yml` (Kafka KRaft + Kafka UI) incluído no projeto.
- README reescrito com comandos para PowerShell, bash e cmd.

### v2: `kafka-echo-v2` 0.0.2-SNAPSHOT

- **`listener.concurrency`** com padrão 3 (`LISTENER_CONCURRENCY`), todas as threads no mesmo `group.id`.
- **Listener em lote com threads virtuais**: `listener.type: batch`; cada evento do lote roda em uma thread
  virtual, com limite de paralelismo por lote (`LISTENER_PARALLELISM`, padrão 50, via semáforo). O método só
  retorna quando o lote termina (commit após o lote). Falha em um evento é logada e o evento é descartado, sem
  reentregar o lote.
- **Rebalance cooperativo**: `CooperativeStickyAssignor`.
- **Parâmetros na inicialização**: `LISTENER_CONCURRENCY`, `MAX_POLL_RECORDS`, `LISTENER_PARALLELISM`,
  `TOPIC_PARTITIONS` (também aceitos como argumentos `--spring.kafka...`).
- Testes do listener reescritos para o formato em lote (paralelismo, limite, isolamento de falhas).

### v3: `kafka-echo-v3` 0.0.3-SNAPSHOT

Resiliência sob stress (itens 1 a 3 da análise de stress):

1. **Publicação protegida**
   - `send` fora das threads do Tomcat, em pool limitado (`PUBLISHER_THREADS`=8, `PUBLISHER_QUEUE`=200).
   - Fila cheia: 503 imediato. Tarefa que esperou na fila mais que `REQUEST_TIMEOUT`: descartada (504).
   - Timeouts do producer: `max.block.ms` 1500, `request.timeout.ms` 3000, `delivery.timeout.ms` 8000
     (todos abaixo dos 10 s do endpoint).
2. **Eventos expirados**: o listener descarta, antes do delay e de qualquer processamento, eventos sem
   requisição pendente (o critério é a ausência da entrada no mapa, removida no timeout). Evita a espiral de backlog.
3. **Readiness, liveness e encerramento**
   - Actuator: `/actuator/health/readiness` só fica `UP` com listener rodando e **todas as partições atribuídas**.
   - `GracefulDrain` (primeira fase de parada): readiness vira `REFUSING_TRAFFIC`, novas requisições recebem 503
     e as pendentes são aguardadas (`DRAIN_TIMEOUT`, 12 s) com o listener Kafka e o servidor web ainda ativos.
   - `server.shutdown: graceful`; exemplo de probes e `preStop` para Kubernetes no README.

### Projeto de carga: `kafka-echo-jmeter` 0.0.1-SNAPSHOT

- Plano `.jmx` em `src/test/jmeter/`, executado por `mvn verify` com `jmeter-maven-plugin` 3.8.0 (não exige
  JMeter instalado), com relatório HTML em `target/jmeter/reports/`.
- Cada requisição sorteia a porta, gera um UUID, usa timeout de 5 s e valida HTTP 200 e `$.message` igual ao
  UUID enviado. Timeout, status diferente de 200 e divergência contam como erro.
- Perfil padrão: 50 threads, ramp-up 10 s, 60 s de duração; tudo parametrizável por `-Djmeter.*`.
- **Correção importante:** a primeira versão usava um pré-processador Groovy (JSR223); o Groovy embutido no
  JMeter 5.6.x não lê classes do Java 25 (`Unsupported class file major version 69`). Foi trocado por funções
  nativas (`__Random`, `__UUID`) em um *User Parameters*.
- A lista de portas (`jmeter.ports`) virou um **intervalo** (`jmeter.portmin` a `jmeter.portmax`), porque as
  funções nativas não separam listas por vírgula.

### Infraestrutura local: `docker-compose.yml`

- Kafka `apache/kafka:4.1.0` em modo KRaft (um nó), listeners interno (`kafka:29092`) e externo (`localhost:9092`),
  healthcheck, volume de dados.
- Kafka UI (`ghcr.io/kafbat/kafka-ui`) em `http://localhost:8090`.

---

## 3. Opções exploradas por tema

Legenda de status: **Adotada** (implementada), **Descartada**, **Não implementada** (analisada, fica como opção).

### 3.1 Mecanismo para "segurar" a requisição até a resposta do Kafka

| Opção | Status | Observações |
|---|---|---|
| `DeferredResult` + registro `ConcurrentHashMap` | **Adotada** (v1 a v3) | Libera a thread do Tomcat; custo pequeno por requisição pendente |
| `CompletableFuture` como retorno do controller | Não implementada | Mesmo mecanismo por baixo; ganho de estilo; timeout e limpeza passam a ser manuais |
| Threads virtuais (`spring.threads.virtual.enabled`) com `future.get(timeout)` | Não implementada | Código síncrono e simples; ainda precisa do mapa; Tomcat continua sem detectar desconexão do cliente |
| WebFlux + Reactor (`Mono`, `Sinks.One`) | Não implementada | Cancelamento chega ao detectar desconexão; reescrita completa; ganho de escala menor com threads virtuais |
| `ReplyingKafkaTemplate` (Spring Kafka) | Não implementada | Substitui registro, correlação e timeout por código testado; o padrão natural usa tópico de resposta; candidata a protótipo (v4) |
| Contrato assíncrono (202 + polling, webhook, SSE) | Não implementada | Elimina a espera síncrona; exige mudar o contrato do BFF |

### 3.2 Onde aplicar o delay de teste

| Opção | Resultado da análise |
|---|---|
| Delay no producer (não bloqueante) | Mais limpo para testar timeout e concorrência de pendentes; não mexe em poll nem rebalance |
| Delay no consumer (`Thread.sleep`) | Simula consumer lento (fila, backpressure), mas ocupa a thread do listener, limita a vazão a `1/delay` e pode estourar `max.poll.interval.ms` |
| Delay do consumer antes do filtro | **Descartada**: multiplicava o atraso pelo número de instâncias |
| Delay do consumer depois do filtro | **Adotada** (v1 ajustada): só eventos da própria instância esperam |
| Delay só em eventos com requisição pendente | **Adotada** (v3) |

Ambos os delays existem na configuração, separados, como pedido.

### 3.3 Paralelismo do consumer

| Opção | Status | Observações |
|---|---|---|
| A. `listener.concurrency` | **Adotada** (v2) | N threads no mesmo `group.id`; teto = número de partições |
| B. Listener em lote + threads virtuais | **Adotada** (v2) | Até 50 eventos por lote em paralelo; tempo do lote = evento mais lento |
| C. Delay assíncrono no consumer (`delayedExecutor`) | Não implementada | Thread nunca bloqueia, mas deixa de simular consumer lento |
| A + B combinadas | **Adotada** | Teto de `concurrency × min(max.poll.records, parallelism)` eventos em voo por instância |

Implicações analisadas: perda de ordem (irrelevante, pois cada evento é independente), `PendingRequestRegistry`
já é thread-safe, commit do lote só após todos terminarem (at-least-once), conclusão idempotente.

### 3.4 Rebalance

| Opção | Status | Observações |
|---|---|---|
| Eager (padrão efetivo: `RangeAssignor`) | Substituída | O padrão do cliente lista `Range` e `CooperativeSticky`; sem configuração prevalece o primeiro |
| `CooperativeStickyAssignor` | **Adotada** (v2) | Só as partições que mudam de dono são movidas; relevante com `concurrency` maior que 1 |
| Protocolo novo (`group.protocol=consumer`, KIP-848) | Não implementada | Suporte no Spring Kafka 4 e restrições de configuração não verificados |
| Readiness após atribuição de partições | **Adotada** (v3) | Ataca o warm-up diretamente |

Riscos mapeados: com `auto-offset-reset: latest` e grupo novo, um rebalance antes do primeiro commit pode fazer a
instância pular eventos da janela (efeito: 504 pontual). O rebalance fica isolado por instância (um grupo por
instância), o que limita seu impacto.

### 3.5 Poll, offsets e ack

| Tema | Situação |
|---|---|
| `max.poll.records` | Padrão do Kafka é 500; **definido como 50** (v1 ajustada) e parametrizável (`MAX_POLL_RECORDS`, v2) |
| `max.poll.interval.ms` | Padrão 5 min; risco com delay alto e lotes grandes, mitigado por lotes menores e delay em paralelo |
| Ack mode | Padrão do Spring Kafka (`BATCH`); `RECORD` ficou como opção |
| Auto commit | Desabilitado pelo Spring (o container confirma o offset) |
| Erro no listener | `DefaultErrorHandler` padrão para falhas não tratadas; na v2/v3 as falhas por evento são capturadas |

### 3.6 Identificador único da instância

| Opção | Status |
|---|---|
| `INSTANCE_ID` explícito | **Adotada** (prioridade 1) |
| `HOSTNAME` (nome do pod no Kubernetes) | **Adotada**, somente quando `KUBERNETES_SERVICE_HOST` existe |
| `HOSTNAME` em qualquer ambiente | **Descartada**: instâncias locais na mesma máquina colidiriam |
| `<app-name>-<6 hex aleatórios>` | **Adotada** (fallback; gerado na subida) |

Risco crítico registrado: duas instâncias com o **mesmo id** passam a ter o mesmo `group.id`, dividem as
partições e eventos são lidos pela instância errada (requisição some e vira timeout).

### 3.7 Infraestrutura local

| Decisão | Motivo |
|---|---|
| Kafka em modo KRaft, sem Zookeeper | O Kafka 4 removeu o Zookeeper; menos peças e subida mais simples; sem impacto na aplicação |
| Kafka UI da kafbat em vez de `provectuslabs/kafka-ui` | O projeto da Provectus está sem manutenção |
| Kafka UI na porta 8090 | Deixa a 8080 livre para a aplicação |
| Compose dentro do projeto | Facilita a subida (`docker compose up -d`) |

### 3.8 Teste de carga (JMeter)

| Opção | Status |
|---|---|
| Sorteio de porta com JSR223/Groovy | **Descartada**: incompatível com Java 25 |
| Sorteio com `__Random` (intervalo `portmin`..`portmax`) e UUID com `__UUID()` | **Adotada** |
| Lista de portas arbitrária | Não suportada (as funções nativas não separam lista por vírgula) |
| `__RandomFromMultipleVars` (separador `\|`) | Analisada; não adotada por ser menos amigável na linha de comando |
| Execução via `jmeter-maven-plugin` | **Adotada** (relatório HTML incluído) |
| Execução com JMeter instalado (`jmeter -n`) | Documentada como alternativa |
| Timeout de 5 s conta como erro | **Adotada** |

Cuidados registrados: o timeout do cliente (5 s) é menor que o da aplicação (10 s); para ver 504 reais é preciso
subir as instâncias com `REQUEST_TIMEOUT` abaixo de 5 s. Esperar alguns segundos após subir as instâncias (warm-up).

### 3.9 Execução com Maven no PowerShell

| Situação | Solução |
|---|---|
| `-Dspring-boot.run.arguments=...` falha com "Unknown lifecycle phase" | Colocar o argumento entre aspas (o PowerShell quebra no ponto) |
| Alternativa mais simples | Variável de ambiente: `$env:PORT=8081; mvn spring-boot:run` |
| `curl` no PowerShell | É alias de `Invoke-WebRequest`; usar `Invoke-RestMethod` ou `curl.exe` |
| Vários `-D` no JMeter | Cada um entre aspas |

### 3.10 Análise de cenários de stress (BFF chamando com 15 s)

Hierarquia de timeouts recomendada: `max.block.ms` < `request.timeout.ms` < `delivery.timeout.ms` ≤ timeout do
endpoint (10 s) < timeout do BFF (15 s) < timeout de proxy/ingress.

| Categoria | Cenários identificados | Tratado |
|---|---|---|
| Timeout | Broker lento bloqueando `send` nas threads do Tomcat; evento publicado tarde; backlog e "espiral da morte"; custo de leitura que cresce com o número de instâncias; warm-up e rebalance; starvation de CPU/GC; pool do Tomcat saturado; pool de conexões do BFF | v3: bulkhead, timeouts do producer, descarte de expirados, readiness. Os demais: monitorar/dimensionar |
| Perda de conexão | Instância cai ou reinicia; parada sem graceful; BFF desiste antes; conexões keep-alive obsoletas; limites de conexão; timeout de proxy | v3: graceful shutdown e dreno. Demais: configuração de infraestrutura e BFF |
| Conflito de requests | Colisão de `correlationId` (desprezível); retry do BFF (amplificação); resposta tardia; **ids de instância duplicados**; reinício de container com mesmo nome de pod; grupos órfãos | Parte coberta por desenho (remove atômico, idempotência); o resto como recomendação |
| Lacunas gerais | Sem limite de requisições pendentes (load shedding); mensagem sem limite de tamanho; erros ambíguos e falta de métricas | Não implementadas |

Já seguros por desenho: UUID v4, registro antes da publicação, remove atômico (corrida entre timeout e listener),
entrega duplicada inofensiva, limpeza do mapa em todos os desfechos, filtro por instância.

---

## 4. Pendências e opções não implementadas

| Item | Origem |
|---|---|
| Load shedding global (limite de requisições pendentes, 429/503) | Análise de stress, item 5 |
| Métricas (pendentes, latência por etapa, lag, eventos descartados) | Análise de stress, item 5 |
| Limite de tamanho da mensagem (`@Size`, 400/413 em vez de 502) | Análise de stress |
| BFF: retry com backoff e jitter, circuit breaker, pool de conexões dimensionado | Análise de stress, item 6 |
| Protótipo v4 com `ReplyingKafkaTemplate` e, se aplicável, tópico de resposta | Alternativas ao `DeferredResult` |
| Partição de resposta dedicada por instância (acaba com a leitura de 100% do tráfego por instância) | Limite estrutural do requisito de grupos distintos |
| `ack-mode: record` | Opções de commit |
| Protocolo de grupos KIP-848 | Opções de rebalance |
| Lista de portas arbitrária no JMeter | Limitação da versão atual |
| Maven Wrapper (`mvnw`) | Não gerado (exige rede) |
| Compilar, executar testes e rodar a carga em ambiente real | Nunca feito |

---

## 5. Arquivos entregues

| Arquivo | Conteúdo |
|---|---|
| `kafka-echo.zip` | v1 (com ajustes de delay após o filtro e `max-poll-records` 50), compose e README |
| `kafka-echo-v2.zip` | v2: concurrency, lote com threads virtuais, rebalance cooperativo, parâmetros |
| `kafka-echo-v3.zip` | v3: bulkhead de publicação, descarte de expirados, readiness e graceful shutdown |
| `kafka-echo-jmeter.zip` | Projeto de carga com `jmeter-maven-plugin` |
| `docker-compose.yml` | Kafka KRaft + Kafka UI (também dentro dos projetos Java) |
| `kafka-echo-consolidado.md` | Este documento |

## 6. Tabela de parâmetros (v3)

| Variável | Padrão | Uso |
|---|---|---|
| `PORT` | 8080 | porta HTTP |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | brokers |
| `INSTANCE_ID` | (vazio) | força o id da instância |
| `LISTENER_CONCURRENCY` | 3 | threads de consumer por instância |
| `MAX_POLL_RECORDS` | 50 | eventos por `poll()` |
| `LISTENER_PARALLELISM` | 50 | eventos de um lote em paralelo |
| `TOPIC_PARTITIONS` | 3 | partições na criação do tópico |
| `PUBLISHER_THREADS` / `PUBLISHER_QUEUE` | 8 / 200 | pool de publicação |
| `PRODUCER_MAX_BLOCK_MS` | 1500 | `max.block.ms` |
| `PRODUCER_REQUEST_TIMEOUT_MS` | 3000 | `request.timeout.ms` |
| `PRODUCER_DELIVERY_TIMEOUT_MS` | 8000 | `delivery.timeout.ms` |
| `REQUEST_TIMEOUT` | 10s | timeout do endpoint (504) |
| `DRAIN_TIMEOUT` | 12s | espera por pendentes no encerramento |
| `SHUTDOWN_PHASE_TIMEOUT` | 20s | limite de cada fase de encerramento |
| `PRODUCER_DELAY` | 0ms | atraso de teste do producer (não bloqueante) |
| `CONSUMER_DELAY` | 0ms | atraso de teste do consumer (`Thread.sleep`) |

Códigos de resposta (v3): 200, 400 (mensagem em branco), 502 (falha ao publicar), 503 (fila cheia ou instância
em encerramento), 504 (sem resposta no prazo ou publicação expirada na fila).
