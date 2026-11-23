package com.exemplo.kafkaecho.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.exemplo.kafkaecho.TestProps;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;

@ExtendWith(MockitoExtension.class)
class KafkaAssignmentHealthIndicatorTest {

    @Mock
    private KafkaListenerEndpointRegistry registry;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private MessageListenerContainer container;

    private KafkaAssignmentHealthIndicator indicator;

    @BeforeEach
    void setUp() {
        indicator = new KafkaAssignmentHealthIndicator(registry, kafkaTemplate, TestProps.props());
    }

    private static List<PartitionInfo> partitionInfos(int count) {
        List<PartitionInfo> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(new PartitionInfo(TestProps.TOPIC, i, null, null, null));
        }
        return list;
    }

    private static List<TopicPartition> assigned(int count) {
        List<TopicPartition> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(new TopicPartition(TestProps.TOPIC, i));
        }
        return list;
    }

    @Test
    void upWhenAllPartitionsAreAssigned() {
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isRunning()).thenReturn(true);
        when(container.getAssignedPartitions()).thenReturn(assigned(3));
        when(kafkaTemplate.partitionsFor(TestProps.TOPIC)).thenReturn(partitionInfos(3));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void outOfServiceWhileOnlySomePartitionsAreAssigned() {
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isRunning()).thenReturn(true);
        when(container.getAssignedPartitions()).thenReturn(assigned(2));
        when(kafkaTemplate.partitionsFor(TestProps.TOPIC)).thenReturn(partitionInfos(3));

        assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void outOfServiceWhenContainerIsNotRunning() {
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isRunning()).thenReturn(false);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void outOfServiceWhenThereAreNoListeners() {
        when(registry.getListenerContainers()).thenReturn(List.of());

        assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void outOfServiceWhenPartitionLookupFails() {
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isRunning()).thenReturn(true);
        when(kafkaTemplate.partitionsFor(TestProps.TOPIC)).thenThrow(new IllegalStateException("broker down"));

        assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void cachesPartitionCountBetweenChecks() {
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isRunning()).thenReturn(true);
        when(container.getAssignedPartitions()).thenReturn(assigned(3));
        when(kafkaTemplate.partitionsFor(TestProps.TOPIC)).thenReturn(partitionInfos(3));

        indicator.health();
        indicator.health();

        verify(kafkaTemplate, times(1)).partitionsFor(TestProps.TOPIC);
    }
}
