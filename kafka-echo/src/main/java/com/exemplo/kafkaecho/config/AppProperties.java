package com.exemplo.kafkaecho.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        String instanceId,
        Kafka kafka,
        Request request,
        TestDelay testDelay,
        Listener listener,
        Producer producer,
        Shutdown shutdown) {

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

    /**
     * @param batchParallelism máximo de eventos de um mesmo lote (poll) processados em paralelo
     */
    public record Listener(int batchParallelism) {
    }

    /**
     * @param threads       threads do pool de publicação (fora das threads do Tomcat)
     * @param queueCapacity tamanho da fila do pool; cheia = rejeição imediata (503)
     */
    public record Producer(int threads, int queueCapacity) {
    }

    /**
     * @param drainTimeout tempo máximo aguardando requisições pendentes no encerramento
     */
    public record Shutdown(Duration drainTimeout) {
    }
}
