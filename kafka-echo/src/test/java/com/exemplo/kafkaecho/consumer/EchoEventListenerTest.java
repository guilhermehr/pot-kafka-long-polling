package com.exemplo.kafkaecho.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.exemplo.kafkaecho.TestProps;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import com.exemplo.kafkaecho.web.EchoResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.context.request.async.DeferredResult;

class EchoEventListenerTest {

    private PendingRequestRegistry registry = new PendingRequestRegistry();
    private final List<EchoEventListener> created = new ArrayList<>();

    @AfterEach
    void tearDown() {
        created.forEach(EchoEventListener::shutdown);
    }

    private EchoEventListener listener(Duration consumerDelay, int parallelism) {
        EchoEventListener listener = new EchoEventListener(
                registry, TestProps.props(Duration.ZERO, consumerDelay, parallelism), TestProps.INSTANCE);
        created.add(listener);
        return listener;
    }

    private EchoEventListener listener(Duration consumerDelay) {
        return listener(consumerDelay, 50);
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

    @SafeVarargs
    private static List<ConsumerRecord<String, String>> batch(ConsumerRecord<String, String>... records) {
        return List.of(records);
    }

    @Test
    void eventFromThisInstanceCompletesPendingRequestWithPayload() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        listener(Duration.ZERO).onEvents(batch(event(TestProps.INSTANCE.id(), "corr-1", "hello")));

        assertThat(deferred.getResult()).isEqualTo(new EchoResponse("hello"));
        assertThat(registry.size()).isZero();
    }

    @Test
    void eventFromAnotherInstanceIsIgnored() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        listener(Duration.ZERO).onEvents(batch(event("kafka-echo-outra", "corr-1", "hello")));

        assertThat(deferred.hasResult()).isFalse();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void unknownCorrelationIdDoesNotThrow() {
        assertThatCode(() -> listener(Duration.ZERO)
                .onEvents(batch(event(TestProps.INSTANCE.id(), "desconhecido", "x"))))
                .doesNotThrowAnyException();
    }

    @Test
    void missingInstanceHeaderIsIgnored() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        listener(Duration.ZERO).onEvents(batch(event(null, "corr-1", "hello")));

        assertThat(deferred.hasResult()).isFalse();
    }

    @Test
    void missingCorrelationHeaderIsIgnored() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        assertThatCode(() -> listener(Duration.ZERO)
                .onEvents(batch(event(TestProps.INSTANCE.id(), null, "hello"))))
                .doesNotThrowAnyException();
        assertThat(deferred.hasResult()).isFalse();
    }

    @Test
    void discardsEventWithoutPendingRequestWithoutApplyingDelay() {
        long start = System.nanoTime();
        listener(Duration.ofMillis(500)).onEvents(batch(event(TestProps.INSTANCE.id(), "expirado", "hello")));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofMillis(300));
    }

    @Test
    void backlogOfExpiredEventsIsDrainedQuicklyAndLiveEventsStillProcessed() {
        DeferredResult<EchoResponse> live = new DeferredResult<>();
        registry.register("vivo", live);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            records.add(event(TestProps.INSTANCE.id(), "expirado-" + i, "x"));
        }
        records.add(event(TestProps.INSTANCE.id(), "vivo", "ok"));

        long start = System.nanoTime();
        listener(Duration.ofMillis(300), 2).onEvents(records);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        // 40 expirados não pagam o delay (com paralelismo 2, seriam 6 s); só o vivo paga 300 ms
        assertThat(elapsed).isLessThan(Duration.ofMillis(1500));
        assertThat(live.getResult()).isEqualTo(new EchoResponse("ok"));
    }

    @Test
    void emptyBatchIsANoOp() {
        assertThatCode(() -> listener(Duration.ZERO).onEvents(List.of())).doesNotThrowAnyException();
    }

    @Test
    void doesNotApplyDelayToEventsFromOtherInstances() {
        long start = System.nanoTime();
        listener(Duration.ofMillis(500)).onEvents(batch(event("kafka-echo-outra", "corr-1", "hello")));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofMillis(300));
    }

    @Test
    void appliesConfiguredDelayToEventsFromThisInstance() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("corr-1", deferred);

        long start = System.nanoTime();
        listener(Duration.ofMillis(200)).onEvents(batch(event(TestProps.INSTANCE.id(), "corr-1", "hello")));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(190));
        assertThat(deferred.hasResult()).isTrue();
    }

    @Test
    void processesBatchEventsInParallel() {
        int events = 10;
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        List<DeferredResult<EchoResponse>> deferreds = new ArrayList<>();
        for (int i = 0; i < events; i++) {
            DeferredResult<EchoResponse> deferred = new DeferredResult<>();
            registry.register("corr-" + i, deferred);
            deferreds.add(deferred);
            records.add(event(TestProps.INSTANCE.id(), "corr-" + i, "msg-" + i));
        }

        long start = System.nanoTime();
        listener(Duration.ofMillis(300), 50).onEvents(records);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        // em série seriam 10 x 300 ms = 3 s
        assertThat(elapsed).isLessThan(Duration.ofMillis(1500));
        for (int i = 0; i < events; i++) {
            assertThat(deferreds.get(i).getResult()).isEqualTo(new EchoResponse("msg-" + i));
        }
    }

    @Test
    void respectsBatchParallelismLimit() {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            registry.register("corr-" + i, new DeferredResult<>());
            records.add(event(TestProps.INSTANCE.id(), "corr-" + i, "msg-" + i));
        }

        long start = System.nanoTime();
        listener(Duration.ofMillis(300), 2).onEvents(records);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        // 4 eventos, 2 por vez: duas "ondas" de 300 ms
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(580));
        assertThat(registry.size()).isZero();
    }

    @Test
    void failureInOneEventDoesNotPreventOthersAndDoesNotThrow() {
        registry = spy(new PendingRequestRegistry());
        DeferredResult<EchoResponse> good = new DeferredResult<>();
        registry.register("bad", new DeferredResult<>());
        registry.register("good", good);
        doThrow(new RuntimeException("boom")).when(registry).complete(eq("bad"), any());

        assertThatCode(() -> listener(Duration.ZERO).onEvents(batch(
                event(TestProps.INSTANCE.id(), "bad", "x"),
                event(TestProps.INSTANCE.id(), "good", "ok"))))
                .doesNotThrowAnyException();

        assertThat(good.getResult()).isEqualTo(new EchoResponse("ok"));
    }
}
