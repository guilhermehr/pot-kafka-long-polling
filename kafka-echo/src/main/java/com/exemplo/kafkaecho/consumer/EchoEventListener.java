package com.exemplo.kafkaecho.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.config.ServiceInstance;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listener em lote: cada poll() entrega uma lista de eventos, processada em paralelo com threads
 * virtuais (limitada por {@code app.listener.batch-parallelism}). O método só retorna quando todos os
 * eventos do lote terminaram; só então o container confirma (commit) o offset do lote.
 */
@Component
public class EchoEventListener {

    private static final Logger log = LoggerFactory.getLogger(EchoEventListener.class);

    private final PendingRequestRegistry registry;
    private final AppProperties props;
    private final ServiceInstance instance;
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("echo-batch-", 0).factory());

    public EchoEventListener(PendingRequestRegistry registry, AppProperties props, ServiceInstance instance) {
        this.registry = registry;
        this.props = props;
        this.instance = instance;
    }

    /**
     * group.id distinto por instância: todas as instâncias recebem todos os eventos, mas só a
     * instância que originou a requisição (header service-instance-id) a conclui.
     */
    @KafkaListener(topics = "${app.kafka.topic}", groupId = "#{@serviceInstance.groupId()}")
    public void onEvents(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) {
            return;
        }
        Semaphore permits = new Semaphore(Math.max(1, props.listener().batchParallelism()));
        List<Future<?>> futures = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> record : records) {
            try {
                permits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Thread do listener interrompida; lote não será totalmente processado");
                break;
            }
            futures.add(executor.submit(() -> {
                try {
                    processSafely(record);
                } finally {
                    permits.release();
                }
            }));
        }
        awaitAll(futures);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }

    /** Uma falha em um evento não pode reentregar (e reprocessar) o lote inteiro. */
    private void processSafely(ConsumerRecord<String, String> record) {
        try {
            handle(record);
        } catch (RuntimeException e) {
            log.error("Erro ao processar evento (partition={}, offset={}); evento descartado",
                    record.partition(), record.offset(), e);
        }
    }

    void handle(ConsumerRecord<String, String> record) {
        AppProperties.HeaderNames names = props.kafka().headers();
        String targetInstance = headerValue(record, names.serviceInstanceId());
        if (!instance.id().equals(targetInstance)) {
            log.debug("Evento de outra instância ({}), ignorado", targetInstance);
            return;
        }

        String correlationId = headerValue(record, names.correlationId());
        if (correlationId == null) {
            log.warn("Evento desta instância sem o header {}, ignorado", names.correlationId());
            return;
        }

        // Evento expirado: a requisição já terminou (timeout, cancelamento) ou nunca existiu aqui
        // (ex.: backlog de uma execução anterior com o mesmo id). Descarta sem delay nem processamento.
        if (!registry.contains(correlationId)) {
            log.debug("Nenhuma requisição pendente para {} (expirada ou duplicada), descartado", correlationId);
            return;
        }

        // Delay de teste só para eventos desta instância com requisição pendente.
        if (!applyTestDelay(props.testDelay().consumer())) {
            return;
        }

        if (registry.complete(correlationId, record.value())) {
            log.debug("Requisição {} concluída", correlationId);
        } else {
            log.debug("Requisição {} expirou durante o processamento", correlationId);
        }
    }

    private static void awaitAll(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                futures.forEach(f -> f.cancel(true));
                return;
            } catch (ExecutionException e) {
                log.error("Falha inesperada no processamento do lote", e.getCause());
            }
        }
    }

    private static String headerValue(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** @return false se a thread foi interrompida durante o delay */
    private static boolean applyTestDelay(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            return true;
        }
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
