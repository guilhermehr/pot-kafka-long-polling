package com.exemplo.kafkaecho;

import java.time.Duration;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.config.ServiceInstance;

public final class TestProps {

    public static final String TOPIC = "echo-events";
    public static final String CORRELATION_HEADER = "x-correlation-id";
    public static final String INSTANCE_HEADER = "x-service-instance-id";
    public static final ServiceInstance INSTANCE = new ServiceInstance("kafka-echo-abc123", "echo-consumer-kafka-echo-abc123");

    private TestProps() {
    }

    public static AppProperties props(Duration producerDelay, Duration consumerDelay) {
        return new AppProperties(
                INSTANCE.id(),
                new AppProperties.Kafka(TOPIC, 3, 1, "echo-consumer",
                        new AppProperties.HeaderNames(CORRELATION_HEADER, INSTANCE_HEADER)),
                new AppProperties.Request(Duration.ofSeconds(10)),
                new AppProperties.TestDelay(producerDelay, consumerDelay));
    }

    public static AppProperties props() {
        return props(Duration.ZERO, Duration.ZERO);
    }
}
