package com.exemplo.kafkaecho.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

class InstanceIdResolverTest {

    @Test
    void usesConfiguredValueWhenPresent() {
        String id = InstanceIdResolver.resolve("meu-id", "kafka-echo",
                Map.of("KUBERNETES_SERVICE_HOST", "10.0.0.1", "HOSTNAME", "pod-1"), () -> "rnd");

        assertThat(id).isEqualTo("meu-id");
    }

    @Test
    void usesHostnameWhenRunningInKubernetes() {
        String id = InstanceIdResolver.resolve("", "kafka-echo",
                Map.of("KUBERNETES_SERVICE_HOST", "10.0.0.1", "HOSTNAME", "kafka-echo-7d9f8b6c5-x2x9z"),
                () -> "rnd");

        assertThat(id).isEqualTo("kafka-echo-7d9f8b6c5-x2x9z");
    }

    @Test
    void ignoresHostnameOutsideKubernetes() {
        String id = InstanceIdResolver.resolve(null, "kafka-echo", Map.of("HOSTNAME", "meu-notebook"), () -> "a1b2c3");

        assertThat(id).isEqualTo("kafka-echo-a1b2c3");
    }

    @Test
    void generatesDifferentIdsForDifferentRandomSuffixes() {
        String first = InstanceIdResolver.resolve(null, "kafka-echo", Map.of(), () -> "aaaaaa");
        String second = InstanceIdResolver.resolve(null, "kafka-echo", Map.of(), () -> "bbbbbb");

        assertThat(first).isNotEqualTo(second);
    }
}
