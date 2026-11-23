package com.exemplo.kafkaecho.correlation;

import java.util.concurrent.ConcurrentHashMap;

import com.exemplo.kafkaecho.web.EchoResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.async.DeferredResult;

/**
 * Requisições HTTP aguardando resposta do Kafka nesta instância
 * (correlationId -> DeferredResult), em um {@link ConcurrentHashMap}.
 */
@Component
public class PendingRequestRegistry {

    private final ConcurrentHashMap<String, DeferredResult<EchoResponse>> pending = new ConcurrentHashMap<>();

    public void register(String correlationId, DeferredResult<EchoResponse> deferred) {
        if (pending.putIfAbsent(correlationId, deferred) != null) {
            throw new IllegalStateException("correlationId já registrado: " + correlationId);
        }
    }

    /** @return true se ainda há requisição aguardando este id (não expirada, não concluída) */
    public boolean contains(String correlationId) {
        return pending.containsKey(correlationId);
    }

    /**
     * Conclui a requisição com a mensagem recebida. {@code remove} é atômico: se o listener e o
     * timeout disputarem o mesmo id, apenas um deles obtém o DeferredResult.
     *
     * @return true se havia requisição pendente e ela foi concluída
     */
    public boolean complete(String correlationId, String message) {
        DeferredResult<EchoResponse> deferred = pending.remove(correlationId);
        return deferred != null && deferred.setResult(new EchoResponse(message));
    }

    /** Conclui a requisição com erro. */
    public boolean fail(String correlationId, Throwable error) {
        DeferredResult<EchoResponse> deferred = pending.remove(correlationId);
        return deferred != null && deferred.setErrorResult(error);
    }

    public void remove(String correlationId) {
        pending.remove(correlationId);
    }

    public int size() {
        return pending.size();
    }
}
