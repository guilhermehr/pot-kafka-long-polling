# kafka-echo-jmeter

Teste de carga do `kafka-echo` (concorrência e performance).

Cada requisição:

1. sorteia aleatoriamente uma porta entre `PORT_MIN` e `PORT_MAX`, inclusive (as instâncias do serviço);
2. gera um UUID novo e envia `POST /api/echo` com `{"message":"<uuid>"}`;
3. usa timeout de 5 s (conexão e resposta);
4. valida **HTTP 200** e que `$.message` da resposta é **igual ao UUID enviado**.

Timeout, status diferente de 200 ou mensagem divergente contam como erro. O nome de cada requisição inclui a
porta (`POST /api/echo [8081]`), então o relatório mostra os números por instância.

## Pré-requisitos

- JDK 17+ e Maven 3.9+ (o plugin baixa o JMeter sozinho; não precisa instalá-lo)
- Kafka no ar (`docker compose up -d` na pasta do `kafka-echo`)
- **Todas as instâncias do kafka-echo já iniciadas**, uma em cada porta do intervalo, por exemplo:

```powershell
# no projeto kafka-echo, um terminal por instância
$env:PORT=8081; mvn spring-boot:run
$env:PORT=8082; mvn spring-boot:run
```

Espere alguns segundos após subir as instâncias antes de começar o teste: no início os consumers ainda estão
recebendo as partições e as primeiras requisições podem dar timeout.

## Executar (Maven)

```bash
mvn verify
```

Parâmetros (padrão entre parênteses):

| Propriedade | Padrão | Descrição |
|---|---|---|
| `jmeter.host` | localhost | host das instâncias |
| `jmeter.portmin` | 8081 | menor porta sorteada |
| `jmeter.portmax` | 8082 | maior porta sorteada (inclusive) |
| `jmeter.threads` | 50 | usuários virtuais |
| `jmeter.rampup` | 10 | subida (s) |
| `jmeter.duration` | 60 | duração (s) |
| `jmeter.timeoutms` | 5000 | timeout da requisição (ms) |
| `jmeter.ignore.failures` | false | true = não falha o build se houver erros |

As portas precisam ser contíguas (o sorteio é entre `portmin` e `portmax`).

**PowerShell** (cada `-D` entre aspas)

```powershell
# padrão: 50 threads, 60 s, portas 8081 e 8082
mvn verify

# três instâncias (8081 a 8083), 100 threads, 2 minutos
mvn verify "-Djmeter.portmax=8083" "-Djmeter.threads=100" "-Djmeter.duration=120"

# teste rápido para validar o ambiente
mvn verify "-Djmeter.threads=5" "-Djmeter.rampup=2" "-Djmeter.duration=15"
```

**bash / cmd**

```bash
mvn verify -Djmeter.portmax=8083 -Djmeter.threads=100 -Djmeter.duration=120
```

Por padrão o build falha (`BUILD FAILURE`) se houver qualquer requisição com erro; o relatório é gerado
mesmo assim. Use `"-Djmeter.ignore.failures=true"` para só observar.

## Resultados

- Relatório HTML: `target/jmeter/reports/kafka-echo-load-test/index.html`
- Resultados brutos (CSV): `target/jmeter/results/`

No relatório, confira: *Errors* (deve ser 0%), *Response Times Percentiles* (p90/p95/p99),
*Throughput* e o *Statistics* por sampler (uma linha por porta).

## Executar com JMeter instalado (alternativa)

```bash
jmeter -n -t src/test/jmeter/kafka-echo-load-test.jmx \
  -JHOST=localhost -JPORT_MIN=8081 -JPORT_MAX=8082 -JTHREADS=50 -JRAMP_UP=10 -JDURATION=60 -JTIMEOUT_MS=5000 \
  -l results/run1.jtl -e -o results/report-run1
```

Rode sempre em modo não-GUI (`-n`) para medir desempenho. Para editar o plano, abra o `.jmx` na GUI.

## Compatibilidade com Java 25

O plano **não usa scripts Groovy/JSR223**: o Groovy embutido no JMeter 5.6.x não lê classes compiladas por
Java 25 (erro `Unsupported class file major version 69`). Porta e UUID são gerados com as funções nativas
`__Random` e `__UUID` (elemento *User Parameters*), então o teste roda com qualquer JDK.

## Cenários úteis

- Carga leve / média / estresse: `jmeter.threads` = 10 / 50 / 200+.
- Com a aplicação subida com `CONSUMER_DELAY=1s` ou `PRODUCER_DELAY=500ms`, veja o impacto na latência
  (PowerShell: `$env:CONSUMER_DELAY="1s"; $env:PORT=8081; mvn spring-boot:run`).
- Para ver 504 vindos da aplicação (e não timeout do cliente), suba as instâncias com `REQUEST_TIMEOUT=3s`
  (menor que o timeout de 5 s do JMeter).
