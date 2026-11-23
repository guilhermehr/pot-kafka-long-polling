package com.exemplo.kafkaecho.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        String instanceId,
        Kafka kafka,
        Request request,
        TestDelay testDelay) {

    public record Kafka(
            String topic,
            int partitions,
            int replicationFactor,
            String groupPrefix,
            HeaderNames headers) {
    }

    public record HeaderNames(String correlationId, String serviceInstanceId) {
    }

    public record Request(Duration timeout) {
    }

    public record TestDelay(Duration producer, Duration consumer) {
    }
}
