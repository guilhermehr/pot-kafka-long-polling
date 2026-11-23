package com.exemplo.kafkaecho.web;

import java.util.UUID;
import java.util.concurrent.CompletionException;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import com.exemplo.kafkaecho.producer.EchoProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
public class EchoController {

    private final PendingRequestRegistry registry;
    private final EchoProducerService producer;
    private final long timeoutMillis;

    public EchoController(PendingRequestRegistry registry,
                          EchoProducerService producer,
                          AppProperties props) {
        this.registry = registry;
        this.producer = producer;
        this.timeoutMillis = props.request().timeout().toMillis();
    }

    @PostMapping(value = "/echo",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public DeferredResult<EchoResponse> echo(@Valid @RequestBody EchoRequest request) {
        String correlationId = UUID.randomUUID().toString();

        DeferredResult<EchoResponse> deferred = new DeferredResult<>(timeoutMillis,
                new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT,
                        "Tempo esgotado aguardando a resposta do Kafka"));
        // Evita vazamento de memória em qualquer desfecho.
        deferred.onTimeout(() -> registry.remove(correlationId));
        deferred.onError(error -> registry.remove(correlationId));
        deferred.onCompletion(() -> registry.remove(correlationId));

        // Registrar ANTES de publicar: a resposta nunca pode chegar antes do registro.
        registry.register(correlationId, deferred);

        producer.publish(correlationId, request.message()).whenComplete((ignored, error) -> {
            if (error != null) {
                Throwable cause = error instanceof CompletionException && error.getCause() != null
                        ? error.getCause() : error;
                registry.fail(correlationId, new ResponseStatusException(
                        HttpStatus.BAD_GATEWAY, "Falha ao publicar no Kafka", cause));
            }
        });

        return deferred;
    }
}
