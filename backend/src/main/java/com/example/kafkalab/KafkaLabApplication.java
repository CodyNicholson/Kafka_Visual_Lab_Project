package com.example.kafkalab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaLabApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaLabApplication.class, args);
    }
}
