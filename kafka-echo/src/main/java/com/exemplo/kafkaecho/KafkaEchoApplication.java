package com.exemplo.kafkaecho;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaEchoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaEchoApplication.class, args);
    }
}
