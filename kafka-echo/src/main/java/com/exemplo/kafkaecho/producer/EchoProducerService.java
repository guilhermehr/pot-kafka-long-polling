package com.exemplo.kafkaecho.producer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.config.ServiceInstance;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EchoProducerService {

    private static final Logger log = LoggerFactory.getLogger(EchoProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AppProperties props;
    private final ServiceInstance instance;

    public EchoProducerService(KafkaTemplate<String, String> kafkaTemplate,
                               AppProperties props,
                               ServiceInstance instance) {
        this.kafkaTemplate = kafkaTemplate;
        this.props = props;
        this.instance = instance;
    }

    /**
     * Publica o evento no tópico. Headers: id da requisição e id da instância; payload: a mensagem.
     * O delay de teste do producer (se houver) é aplicado sem bloquear a thread chamadora.
     */
    public CompletableFuture<Void> publish(String correlationId, String message) {
        ProducerRecord<String, String> record = buildRecord(correlationId, message);
        return CompletableFuture.runAsync(() -> { }, delayedExecutor(props.testDelay().producer()))
                .thenCompose(ignored -> kafkaTemplate.send(record))
                .thenAccept(result -> log.debug("Evento publicado: topic={}, correlationId={}",
                        record.topic(), correlationId));
    }

    ProducerRecord<String, String> buildRecord(String correlationId, String message) {
        AppProperties.HeaderNames headers = props.kafka().headers();
        ProducerRecord<String, String> record =
                new ProducerRecord<>(props.kafka().topic(), correlationId, message);
        record.headers().add(headers.correlationId(), correlationId.getBytes(StandardCharsets.UTF_8));
        record.headers().add(headers.serviceInstanceId(), instance.id().getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static Executor delayedExecutor(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            return Runnable::run;
        }
        return CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS);
    }
}
