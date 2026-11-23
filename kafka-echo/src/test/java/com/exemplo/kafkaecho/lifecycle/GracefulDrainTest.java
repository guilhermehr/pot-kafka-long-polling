package com.exemplo.kafkaecho.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.exemplo.kafkaecho.TestProps;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.web.context.request.async.DeferredResult;

class GracefulDrainTest {

    private final PendingRequestRegistry registry = new PendingRequestRegistry();
    private final List<Object> events = new ArrayList<>();
    private final ApplicationEventPublisher publisher = events::add;

    private GracefulDrain drain(Duration drainTimeout) {
        return new GracefulDrain(registry, publisher, TestProps.full(
                Duration.ZERO, Duration.ZERO, 50, Duration.ofSeconds(10), 8, 200, drainTimeout));
    }

    @Test
    void stopSwitchesReadinessToRefusingTraffic() {
        GracefulDrain drain = drain(Duration.ofSeconds(1));
        drain.start();

        drain.stop();

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOfSatisfying(AvailabilityChangeEvent.class,
                event -> assertThat(event.getState()).isEqualTo(ReadinessState.REFUSING_TRAFFIC));
        assertThat(drain.isRunning()).isFalse();
    }

    @Test
    void stopReturnsImmediatelyWhenNothingIsPending() {
        long start = System.nanoTime();
        drain(Duration.ofSeconds(5)).stop();

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(500));
    }

    @Test
    void stopWaitsForPendingRequestsToFinish() throws Exception {
        registry.register("corr-1", new DeferredResult<>());
        Thread completer = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            registry.complete("corr-1", "ok");
        });

        long start = System.nanoTime();
        drain(Duration.ofSeconds(5)).stop();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        completer.join();

        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(250)).isLessThan(Duration.ofSeconds(3));
        assertThat(registry.size()).isZero();
    }

    @Test
    void stopGivesUpAfterDrainTimeout() {
        registry.register("corr-1", new DeferredResult<>());

        long start = System.nanoTime();
        drain(Duration.ofMillis(300)).stop();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(280)).isLessThan(Duration.ofSeconds(2));
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void runsInTheFirstShutdownPhase() {
        assertThat(drain(Duration.ofSeconds(1)).getPhase()).isEqualTo(Integer.MAX_VALUE);
    }
}
