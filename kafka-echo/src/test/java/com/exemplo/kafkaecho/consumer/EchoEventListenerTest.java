package com.exemplo.kafkaecho.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.exemplo.kafkaecho.TestProps;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import com.exemplo.kafkaecho.web.EchoResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.web.context.request.async.DeferredResult;

class EchoEventListenerTest {

    private final PendingRequestRegistry registry = new PendingRequestRegistry();

    private EchoEventListener listener(Duration consumerDelay) {
        return new EchoEventListener(registry, TestProps.props(Duration.ZERO, consumerDelay), TestProps.INSTANCE);
    }

    private static ConsumerRecord<String, String> event(String instanceId, String correlationId, String payload) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(TestProps.TOPIC, 0, 0L, correlationId, payload);
        if (instanceId != null) {
            record.headers().add(TestProps.INSTANCE_HEADER, instanceId.getBytes(StandardCharsets.UTF_8));
        }
        if (correlationId != null) {
            record.headers().add(TestProps.CORRELATION_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Test
    void eventFromThisInstanceCompletesPendingRequestWithPayload() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        listener(Duration.ZERO).onEvent(event(TestProps.INSTANCE.id(), "corr-1", "hello"));

        assertThat(deferred.getResult()).isEqualTo(new EchoResponse("hello"));
        assertThat(registry.size()).isZero();
    }

    @Test
    void eventFromAnotherInstanceIsIgnored() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        listener(Duration.ZERO).onEvent(event("kafka-echo-outra", "corr-1", "hello"));

        assertThat(deferred.hasResult()).isFalse();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void unknownCorrelationIdDoesNotThrow() {
        assertThatCode(() -> listener(Duration.ZERO).onEvent(event(TestProps.INSTANCE.id(), "desconhecido", "x")))
                .doesNotThrowAnyException();
    }

    @Test
    void missingInstanceHeaderIsIgnored() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        listener(Duration.ZERO).onEvent(event(null, "corr-1", "hello"));

        assertThat(deferred.hasResult()).isFalse();
    }

    @Test
    void missingCorrelationHeaderIsIgnored() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        assertThatCode(() -> listener(Duration.ZERO).onEvent(event(TestProps.INSTANCE.id(), null, "hello")))
                .doesNotThrowAnyException();
        assertThat(deferred.hasResult()).isFalse();
    }

    @Test
    void doesNotApplyDelayToEventsFromOtherInstances() {
        long start = System.nanoTime();
        listener(Duration.ofMillis(500)).onEvent(event("kafka-echo-outra", "corr-1", "hello"));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofMillis(300));
    }

    @Test
    void appliesConfiguredDelayToEventsFromThisInstance() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        long start = System.nanoTime();
        listener(Duration.ofMillis(200)).onEvent(event(TestProps.INSTANCE.id(), "corr-1", "hello"));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(190));
        assertThat(deferred.hasResult()).isTrue();
    }
}
