package com.example.kafkalab.model;

public record TraceEvent(
        String correlationId,
        String stage,          // PRODUCER_SENT, BROKER_ACK, CONSUMER_A, CONSUMER_B, RETRY, DLT, REPLAY_TRIGGERED
        String topic,
        String key,
        String payload,
        Integer partition,
        Long offset,
        String consumerGroup,
        String consumerThread,
        String description,
        long timestamp
) {}
