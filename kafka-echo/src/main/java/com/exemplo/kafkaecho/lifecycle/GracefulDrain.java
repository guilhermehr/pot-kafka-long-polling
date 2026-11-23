package com.exemplo.kafkaecho.lifecycle;

import java.time.Duration;

import com.exemplo.kafkaecho.config.AppProperties;
import com.exemplo.kafkaecho.correlation.PendingRequestRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * No encerramento: (1) passa a readiness para REFUSING_TRAFFIC (o balanceador deixa de enviar tráfego e o
 * controller recusa requisições novas com 503) e (2) aguarda as requisições pendentes terminarem, até
 * {@code app.shutdown.drain-timeout}.
 * <p>
 * A fase é {@link Integer#MAX_VALUE}, a primeira a parar: o dreno acontece enquanto o listener Kafka e o
 * servidor web ainda estão ativos. Sem isso, o listener pararia antes e as requisições em voo ficariam sem
 * como receber a resposta.
 */
@Component
public class GracefulDrain implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GracefulDrain.class);
    private static final long POLL_MILLIS = 50;

    private final PendingRequestRegistry registry;
    private final ApplicationEventPublisher publisher;
    private final Duration drainTimeout;
    private volatile boolean running;

    public GracefulDrain(PendingRequestRegistry registry, ApplicationEventPublisher publisher, AppProperties props) {
        this.registry = registry;
        this.publisher = publisher;
        this.drainTimeout = props.shutdown().drainTimeout();
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        AvailabilityChangeEvent.publish(publisher, this, ReadinessState.REFUSING_TRAFFIC);

        int pending = registry.size();
        if (pending == 0) {
            log.info("Encerrando: nenhuma requisição pendente");
            return;
        }
        log.info("Encerrando: aguardando {} requisição(ões) pendente(s) por até {}", pending, drainTimeout);

        long deadline = System.nanoTime() + drainTimeout.toNanos();
        while (registry.size() > 0 && System.nanoTime() < deadline) {
            try {
                Thread.sleep(POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        int remaining = registry.size();
        if (remaining == 0) {
            log.info("Encerrando: todas as requisições pendentes foram concluídas");
        } else {
            log.warn("Encerrando: {} requisição(ões) ainda pendente(s) após {}", remaining, drainTimeout);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
