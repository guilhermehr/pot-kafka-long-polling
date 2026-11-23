package com.exemplo.kafkaecho.producer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.config.ServiceInstance;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publica os eventos em um pool próprio e limitado (bulkhead). {@code KafkaTemplate.send} pode bloquear
 * por até {@code max.block.ms} (metadados, buffer cheio); fazer isso nas threads do Tomcat derrubaria a
 * aplicação inteira quando o broker estiver lento ou indisponível.
 */
@Service
public class EchoProducerService {

    private static final Logger log = LoggerFactory.getLogger(EchoProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AppProperties props;
    private final ServiceInstance instance;
    private final ThreadPoolExecutor publisher;

    public EchoProducerService(KafkaTemplate<String, String> kafkaTemplate,
                               AppProperties props,
                               ServiceInstance instance) {
        this.kafkaTemplate = kafkaTemplate;
        this.props = props;
        this.instance = instance;

        int threads = Math.max(1, props.producer().threads());
        int queue = Math.max(1, props.producer().queueCapacity());
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "echo-publisher-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
        // Política padrão (AbortPolicy): fila cheia lança RejectedExecutionException.
        this.publisher = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queue), factory);
        this.publisher.allowCoreThreadTimeOut(true);
    }

    /**
     * Publica o evento no tópico. Headers: id da requisição e id da instância; payload: a mensagem.
     * <p>
     * O future conclui com {@link RejectedExecutionException} se a fila de publicação está cheia,
     * com {@link PublishExpiredException} se a tarefa esperou mais que o timeout da requisição, ou com
     * a falha do envio. O delay de teste (se houver) não bloqueia nenhuma thread.
     */
    public CompletableFuture<Void> publish(String correlationId, String message) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        long enqueuedAt = System.nanoTime();
        ProducerRecord<String, String> record = buildRecord(correlationId, message);

        Runnable submit = () -> {
            try {
                publisher.execute(() -> send(record, correlationId, enqueuedAt, result));
            } catch (RejectedExecutionException e) {
                log.warn("Fila de publicação cheia; requisição {} rejeitada", correlationId);
                result.completeExceptionally(e);
            }
        };

        Duration delay = props.testDelay().producer();
        if (delay == null || delay.isZero() || delay.isNegative()) {
            submit.run();
        } else {
            // A tarefa agendada só enfileira (rápido); o envio roda no pool de publicação.
            CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS, Runnable::run).execute(submit);
        }
        return result;
    }

    @PreDestroy
    void shutdown() {
        publisher.shutdown();
    }

    private void send(ProducerRecord<String, String> record,
                      String correlationId,
                      long enqueuedAt,
                      CompletableFuture<Void> result) {
        try {
            Duration waited = Duration.ofNanos(System.nanoTime() - enqueuedAt);
            if (waited.compareTo(props.request().timeout()) >= 0) {
                log.warn("Requisição {} esperou {} ms para publicar (acima do timeout); descartada",
                        correlationId, waited.toMillis());
                result.completeExceptionally(new PublishExpiredException(
                        "Requisição expirou antes da publicação (esperou " + waited.toMillis() + " ms)"));
                return;
            }
            kafkaTemplate.send(record).whenComplete((sendResult, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    log.debug("Evento publicado: topic={}, correlationId={}", record.topic(), correlationId);
                    result.complete(null);
                }
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }

    ProducerRecord<String, String> buildRecord(String correlationId, String message) {
        AppProperties.HeaderNames headers = props.kafka().headers();
        ProducerRecord<String, String> record =
                new ProducerRecord<>(props.kafka().topic(), correlationId, message);
        record.headers().add(headers.correlationId(), correlationId.getBytes(StandardCharsets.UTF_8));
        record.headers().add(headers.serviceInstanceId(), instance.id().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
