package com.exemplo.kafkaecho.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.exemplo.kafkaecho.TestProps;
import org.apache.kafka.clients.producer.ProducerRecord;
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

    private EchoProducerService service(Duration producerDelay) {
        return new EchoProducerService(kafkaTemplate, TestProps.props(producerDelay, Duration.ZERO), TestProps.INSTANCE);
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
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(new IllegalStateException("serializer"));

        CompletableFuture<Void> future = service(Duration.ZERO).publish("corr-1", "hello");

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    void appliesConfiguredDelayBeforeSending() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        long start = System.nanoTime();
        CompletableFuture<Void> future = service(Duration.ofMillis(300)).publish("corr-1", "hello");

        verifyNoInteractions(kafkaTemplate); // ainda não enviou, e a thread chamadora não ficou bloqueada
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(250));

        future.get(3, TimeUnit.SECONDS);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(290));
        capturePublished();
    }
}
