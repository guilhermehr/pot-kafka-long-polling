package com.exemplo.kafkaecho.config;

/**
 * Identidade desta instância do serviço.
 *
 * @param id      identificador único da instância (ex.: kafka-echo-7f3a9c ou nome do pod)
 * @param groupId group.id do consumer Kafka desta instância (único por instância)
 */
public record ServiceInstance(String id, String groupId) {
}
