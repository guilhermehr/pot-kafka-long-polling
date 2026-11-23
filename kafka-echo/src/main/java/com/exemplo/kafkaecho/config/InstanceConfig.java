package com.exemplo.kafkaecho.config;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class InstanceConfig {

    private static final Logger log = LoggerFactory.getLogger(InstanceConfig.class);

    @Bean
    public ServiceInstance serviceInstance(AppProperties props, Environment environment) {
        String applicationName = environment.getProperty("spring.application.name", "kafka-echo");
        String id = InstanceIdResolver.resolve(
                props.instanceId(),
                applicationName,
                System.getenv(),
                () -> UUID.randomUUID().toString().substring(0, 6));
        String groupId = props.kafka().groupPrefix() + "-" + id;
        log.info("Instância iniciada: id={}, consumer group.id={}", id, groupId);
        return new ServiceInstance(id, groupId);
    }
}
