package com.exemplo.kafkaecho.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    /** Criado na subida pelo KafkaAdmin (auto-configurado) caso ainda não exista. */
    @Bean
    public NewTopic echoTopic(AppProperties props) {
        AppProperties.Kafka kafka = props.kafka();
        return TopicBuilder.name(kafka.topic())
                .partitions(kafka.partitions())
                .replicas(kafka.replicationFactor())
                .build();
    }
}
