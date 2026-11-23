package com.exemplo.kafkaecho.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.exemplo.kafkaecho.TestProps;
import com.exemplo.kafkaecho.config.AppProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class EchoProducerServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private final List<EchoProducerService> created = new ArrayList<>();

    @AfterEach
    void tearDown() {
        created.forEach(EchoProducerService::shutdown);
    }

    private EchoProducerService service(AppProperties props) {
        EchoProducerService service = new EchoProducerService(kafkaTemplate, props, TestProps.INSTANCE);
        created.add(service);
        return service;
    }

    private EchoProducerService service(Duration producerDelay) {
        return service(TestProps.props(producerDelay, Duration.ZERO));
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, String> capturePublished() {
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    private static String header(ProducerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void publishesToConfiguredTopicWithHeadersAndPayload() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        service(Duration.ZERO).publish("corr-1", "hello").get(2, TimeUnit.SECONDS);

        ProducerRecord<String, String> record = capturePublished();
        assertThat(record.topic()).isEqualTo(TestProps.TOPIC);
        assertThat(record.value()).isEqualTo("hello");
        assertThat(header(record, TestProps.CORRELATION_HEADER)).isEqualTo("corr-1");
        assertThat(header(record, TestProps.INSTANCE_HEADER)).isEqualTo(TestProps.INSTANCE.id());
    }

    @Test
    void sendsOnPublisherThreadNotOnCallerThread() throws Exception {
        AtomicReference<String> threadName = new AtomicReference<>();
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            threadName.set(Thread.currentThread().getName());
            return CompletableFuture.completedFuture(null);
        });

        service(Duration.ZERO).publish("corr-1", "hello").get(2, TimeUnit.SECONDS);

        assertThat(threadName.get()).startsWith("echo-publisher-").isNotEqualTo(Thread.currentThread().getName());
    }

    @Test
    void sendFailureCompletesFutureExceptionally() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        CompletableFuture<Void> future = service(Duration.ZERO).publish("corr-1", "hello");

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("broker down");
    }

    @Test
    void synchronousSendExceptionCompletesFutureExceptionally() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(new IllegalStateException("max.block.ms"));

        CompletableFuture<Void> future = service(Duration.ZERO).publish("corr-1", "hello");

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("max.block.ms");
    }

    @Test
    void appliesConfiguredDelayBeforeSendingWithoutBlockingTheCaller() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        long start = System.nanoTime();
        CompletableFuture<Void> future = service(Duration.ofMillis(300)).publish("corr-1", "hello");

        verifyNoInteractions(kafkaTemplate);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(250));

        future.get(3, TimeUnit.SECONDS);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(290));
        capturePublished();
    }

    @Test
    void rejectsImmediatelyWhenPublishQueueIsFull() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            release.await();
            return CompletableFuture.completedFuture(null);
        });
        // 1 thread + fila de 1: a 1ª ocupa a thread, a 2ª espera na fila, a 3ª é rejeitada.
        EchoProducerService service = service(TestProps.full(
                Duration.ZERO, Duration.ZERO, 50, Duration.ofSeconds(10), 1, 1, Duration.ofSeconds(1)));

        CompletableFuture<Void> first = service.publish("c1", "m");
        CompletableFuture<Void> second = service.publish("c2", "m");
        CompletableFuture<Void> third = service.publish("c3", "m");
        try {
            assertThat(third).isCompletedExceptionally();
            assertThatThrownBy(third::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class);
        } finally {
            release.countDown();
        }
        first.get(2, TimeUnit.SECONDS);
        second.get(2, TimeUnit.SECONDS);
    }

    @Test
    void discardsPublicationThatWaitedLongerThanRequestTimeout() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            release.await();
            return CompletableFuture.completedFuture(null);
        });
        EchoProducerService service = service(TestProps.full(
                Duration.ZERO, Duration.ZERO, 50, Duration.ofMillis(200), 1, 5, Duration.ofSeconds(1)));

        CompletableFuture<Void> first = service.publish("c1", "m");   // ocupa a única thread
        CompletableFuture<Void> second = service.publish("c2", "m");  // espera na fila além do timeout
        Thread.sleep(350);
        release.countDown();

        first.get(2, TimeUnit.SECONDS);
        assertThatThrownBy(() -> second.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PublishExpiredException.class);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }
}
