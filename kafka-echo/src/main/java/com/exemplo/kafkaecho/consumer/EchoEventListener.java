package com.exemplo.kafkaecho.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.config.ServiceInstance;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EchoEventListener {

    private static final Logger log = LoggerFactory.getLogger(EchoEventListener.class);

    private final PendingRequestRegistry registry;
    private final AppProperties props;
    private final ServiceInstance instance;

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
    public void onEvent(ConsumerRecord<String, String> record) {
        AppProperties.HeaderNames names = props.kafka().headers();
        String targetInstance = headerValue(record, names.serviceInstanceId());
        if (!instance.id().equals(targetInstance)) {
            log.debug("Evento de outra instância ({}), ignorado", targetInstance);
            return;
        }

        // Delay de teste só para eventos desta instância (os de outras já foram descartados acima).
        if (!applyTestDelay(props.testDelay().consumer())) {
            return;
        }

        String correlationId = headerValue(record, names.correlationId());
        if (correlationId == null) {
            log.warn("Evento desta instância sem o header {}, ignorado", names.correlationId());
            return;
        }

        if (registry.complete(correlationId, record.value())) {
            log.debug("Requisição {} concluída", correlationId);
        } else {
            log.debug("Nenhuma requisição pendente para {} (timeout ou duplicado)", correlationId);
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
