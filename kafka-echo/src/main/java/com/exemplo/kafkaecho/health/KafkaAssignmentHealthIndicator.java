package com.exemplo.kafkaecho.health;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.exemplo.kafkaecho.config.AppProperties;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Faz parte do grupo de readiness: a instância só fica "pronta" quando os listeners estão rodando e
 * todas as partições do tópico estão atribuídas a ela. Evita receber tráfego (e responder 504) enquanto o
 * consumer ainda não entrou no grupo ou está em rebalance.
 */
@Component
public class KafkaAssignmentHealthIndicator implements HealthIndicator {

    private static final long PARTITION_COUNT_TTL_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final KafkaListenerEndpointRegistry registry;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    private volatile int cachedPartitions;
    private volatile long cachedAt;

    public KafkaAssignmentHealthIndicator(KafkaListenerEndpointRegistry registry,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          AppProperties props) {
        this.registry = registry;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = props.kafka().topic();
    }

    @Override
    public Health health() {
        Collection<MessageListenerContainer> containers = registry.getListenerContainers();
        if (containers.isEmpty()) {
            return Health.outOfService().withDetail("motivo", "nenhum listener Kafka registrado").build();
        }
        if (containers.stream().anyMatch(container -> !container.isRunning())) {
            return Health.outOfService().withDetail("motivo", "listener Kafka não está em execução").build();
        }

        int expected;
        try {
            expected = expectedPartitions();
        } catch (RuntimeException e) {
            return Health.outOfService()
                    .withDetail("motivo", "não foi possível consultar as partições do tópico")
                    .withDetail("erro", String.valueOf(e.getMessage()))
                    .build();
        }
        if (expected <= 0) {
            return Health.outOfService().withDetail("motivo", "tópico sem partições (ainda não criado?)").build();
        }

        int assigned = containers.stream()
                .map(MessageListenerContainer::getAssignedPartitions)
                .filter(partitions -> partitions != null)
                .mapToInt(Collection<TopicPartition>::size)
                .sum();

        Health.Builder builder = assigned >= expected ? Health.up() : Health.outOfService();
        return builder
                .withDetail("topic", topic)
                .withDetail("assignedPartitions", assigned)
                .withDetail("expectedPartitions", expected)
                .build();
    }

    private int expectedPartitions() {
        long now = System.nanoTime();
        if (cachedPartitions > 0 && now - cachedAt < PARTITION_COUNT_TTL_NANOS) {
            return cachedPartitions;
        }
        List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(topic);
        int count = partitions == null ? 0 : partitions.size();
        cachedPartitions = count;
        cachedAt = now;
        return count;
    }
}
