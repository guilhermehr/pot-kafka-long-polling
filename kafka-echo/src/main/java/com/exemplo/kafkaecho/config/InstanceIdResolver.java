package com.exemplo.kafkaecho.config;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Resolve o identificador único da instância, nesta ordem:
 * <ol>
 *   <li>valor configurado explicitamente (app.instance-id / INSTANCE_ID);</li>
 *   <li>HOSTNAME, quando rodando no Kubernetes (nome do pod, já único);</li>
 *   <li>{@code <application-name>-<sufixo aleatório>}, para várias instâncias locais.</li>
 * </ol>
 * HOSTNAME só é usado no Kubernetes porque, fora dele, várias instâncias na mesma
 * máquina compartilhariam o mesmo valor.
 */
public final class InstanceIdResolver {

    static final String KUBERNETES_MARKER = "KUBERNETES_SERVICE_HOST";
    static final String HOSTNAME = "HOSTNAME";

    private InstanceIdResolver() {
    }

    public static String resolve(String configured,
                                 String applicationName,
                                 Map<String, String> env,
                                 Supplier<String> randomSuffix) {
        if (hasText(configured)) {
            return configured.trim();
        }
        if (hasText(env.get(KUBERNETES_MARKER)) && hasText(env.get(HOSTNAME))) {
            return env.get(HOSTNAME).trim();
        }
        return applicationName + "-" + randomSuffix.get();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
