package com.exemplo.kafkaecho.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.exemplo.kafkaecho.web.EchoResponse;
import org.junit.jupiter.api.Test;
import org.springframework.web.context.request.async.DeferredResult;

class PendingRequestRegistryTest {

    private final PendingRequestRegistry registry = new PendingRequestRegistry();

    @Test
    void completeResolvesDeferredResultAndRemovesEntry() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("id-1", deferred);

        boolean completed = registry.complete("id-1", "hello");

        assertThat(completed).isTrue();
        assertThat(deferred.getResult()).isEqualTo(new EchoResponse("hello"));
        assertThat(registry.size()).isZero();
    }

    @Test
    void containsReflectsPendingState() {
        assertThat(registry.contains("id-1")).isFalse();

        registry.register("id-1", new DeferredResult<>());
        assertThat(registry.contains("id-1")).isTrue();

        registry.complete("id-1", "x");
        assertThat(registry.contains("id-1")).isFalse();
    }

    @Test
    void completeUnknownIdReturnsFalse() {
        assertThat(registry.complete("missing", "x")).isFalse();
    }

    @Test
    void secondCompleteForSameIdReturnsFalse() {
        registry.register("id-1", new DeferredResult<>());

        assertThat(registry.complete("id-1", "a")).isTrue();
        assertThat(registry.complete("id-1", "b")).isFalse();
    }

    @Test
    void removeDropsEntryWithoutCompleting() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("id-1", deferred);

        registry.remove("id-1");

        assertThat(registry.size()).isZero();
        assertThat(deferred.hasResult()).isFalse();
        assertThat(registry.complete("id-1", "x")).isFalse();
    }

    @Test
    void failCompletesDeferredResultWithError() {
        DeferredResult<EchoResponse> deferred = new DeferredResult<>();
        registry.register("id-1", deferred);
        RuntimeException error = new RuntimeException("boom");

        assertThat(registry.fail("id-1", error)).isTrue();
        assertThat(deferred.getResult()).isSameAs(error);
        assertThat(registry.size()).isZero();
    }

    @Test
    void registeringDuplicateIdFails() {
        registry.register("id-1", new DeferredResult<>());

        assertThatThrownBy(() -> registry.register("id-1", new DeferredResult<>()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void concurrentCompletionsOfSameIdSucceedExactlyOnce() throws Exception {
        registry.register("id-1", new DeferredResult<>());
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String value = "v" + i;
            results.add(pool.submit(() -> {
                start.await();
                return registry.complete("id-1", value);
            }));
        }
        start.countDown();

        int successes = 0;
        for (Future<Boolean> f : results) {
            if (f.get()) {
                successes++;
            }
        }
        pool.shutdown();

        assertThat(successes).isEqualTo(1);
    }
}
