package com.example.kafkalab.service;

import com.example.kafkalab.config.KafkaConfig;
import com.example.kafkalab.model.TraceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaDemoService implements ConsumerSeekAware {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TelemetryService telemetry;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    // Current assignment per listener consumer. AbstractConsumerSeekAware (Spring Kafka 3.2) keys callbacks by
    // TopicPartition, so fulfillment-group and analytics-group on the same demo-orders partitions overwrite each other.
    private final ThreadLocal<ConsumerSeekCallback> consumerCallback = new ThreadLocal<>();
    private final Map<ConsumerSeekCallback, Set<TopicPartition>> assignments = new ConcurrentHashMap<>();

    public KafkaDemoService(KafkaTemplate<String, String> kafkaTemplate, TelemetryService telemetry) {
        this.kafkaTemplate = kafkaTemplate;
        this.telemetry = telemetry;
    }

    public void sendDemoMessage(String key, String payload, boolean roundRobinHint) {
        String correlationId = UUID.randomUUID().toString().substring(0, 8);
        String enrichedPayload = correlationId + "|" + payload;

        // 1. Emit Producer Dispatched Event
        telemetry.emit(new TraceEvent(
                correlationId,
                "PRODUCER_SENT",
                KafkaConfig.MAIN_TOPIC,
                key,
                payload,
                null,
                null,
                null,
                "http-nio-producer",
                key == null
                        ? "Producer serialized record with NULL key. Partitioner will distribute across partitions."
                        : "Producer hashed key '" + key + "' via Murmur2 to guarantee strict partition affinity.",
                System.currentTimeMillis()
        ));

        // If keyless demo, cycle partitions explicitly so small single-message clicks visibly rotate 0 -> 1 -> 2
        Integer targetPartition = (key == null && roundRobinHint)
                ? Math.floorMod(roundRobinCounter.getAndIncrement(), 3)
                : null;

        var future = (targetPartition != null)
                ? kafkaTemplate.send(KafkaConfig.MAIN_TOPIC, targetPartition, null, enrichedPayload)
                : kafkaTemplate.send(KafkaConfig.MAIN_TOPIC, key, enrichedPayload);

        future.whenComplete((result, ex) -> {
            if (ex == null && result != null) {
                var meta = result.getRecordMetadata();
                telemetry.emit(new TraceEvent(
                        correlationId,
                        "BROKER_ACK",
                        meta.topic(),
                        key,
                        payload,
                        meta.partition(),
                        meta.offset(),
                        null,
                        "kafka-broker-1",
                        String.format("Broker appended record to %s [Partition %d] at Offset #%d (acks=all).",
                                meta.topic(), meta.partition(), meta.offset()),
                        System.currentTimeMillis()
                ));
            }
        });
    }

    /**
     * CONSUMER GROUP A: Competing Consumers (3 threads sharing 3 partitions)
     * Configured with non-blocking retries and Dead Letter Topic routing.
     * A fixed backoff with one retry produces a single retry topic: demo-orders-retry.
     * Failed records are forwarded to the same partition number, so retry/DLT topics need 3 partitions too.
     */
    @RetryableTopic(
            attempts = "2",
            backoff = @Backoff(delay = 1200),
            numPartitions = "3",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltTopicSuffix = "-dlt"
    )
    @KafkaListener(
            id = "fulfillment-listener",
            topics = KafkaConfig.MAIN_TOPIC,
            groupId = "fulfillment-group",
            concurrency = "3"
    )
    public void consumeFulfillmentGroup(ConsumerRecord<String, String> record) throws InterruptedException {
        Thread.sleep(600); // Slight pause so UI animation flows step-by-step
        String[] parts = parsePayload(record.value());
        String correlationId = parts[0];
        String cleanPayload = parts[1];
        String workerName = "Fulfillment-Worker-" + record.partition();

        if (cleanPayload.contains("POISON") || cleanPayload.contains("FAIL")) {
            telemetry.emit(new TraceEvent(
                    correlationId,
                    "RETRY",
                    record.topic(),
                    record.key(),
                    cleanPayload,
                    record.partition(),
                    record.offset(),
                    "fulfillment-group",
                    workerName,
                    String.format("%s threw RuntimeException on '%s' from %s [Partition %d, Offset #%d]. Scheduling retry/DLT!",
                            workerName, cleanPayload, record.topic(), record.partition(), record.offset()),
                    System.currentTimeMillis()
            ));
            throw new IllegalArgumentException("Simulated Poison Pill failure for payload: " + cleanPayload);
        }

        telemetry.emit(new TraceEvent(
                correlationId,
                "CONSUMER_A",
                record.topic(),
                record.key(),
                cleanPayload,
                record.partition(),
                record.offset(),
                "fulfillment-group",
                workerName,
                String.format("Group 'fulfillment-group' (%s) processed Partition %d, Offset #%d.",
                        workerName, record.partition(), record.offset()),
                System.currentTimeMillis()
        ));
    }

    /**
     * DEAD LETTER TOPIC (DLT) HANDLER
     */
    @DltHandler
    public void handleDlt(
            ConsumerRecord<String, String> record,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage
    ) {
        String[] parts = parsePayload(record.value());
        telemetry.emit(new TraceEvent(
                parts[0],
                "DLT",
                record.topic(),
                record.key(),
                parts[1],
                record.partition(),
                record.offset(),
                "fulfillment-group-dlt",
                "DLT-Quarantine-Handler",
                String.format("Poison pill quarantined in Dead Letter Topic '%s' [Partition %d, Offset #%d]. Reason: %s",
                        record.topic(), record.partition(), record.offset(), exceptionMessage),
                System.currentTimeMillis()
        ));
    }

    /**
     * CONSUMER GROUP B: Independent Pub/Sub Broadcast Group
     * Receives every message independently with its own offset tracking.
     */
    @KafkaListener(
            id = "analytics-listener",
            topics = KafkaConfig.MAIN_TOPIC,
            groupId = "analytics-group",
            concurrency = "1"
    )
    public void consumeAnalyticsGroup(ConsumerRecord<String, String> record) throws InterruptedException {
        Thread.sleep(900);
        String[] parts = parsePayload(record.value());
        telemetry.emit(new TraceEvent(
                parts[0],
                "CONSUMER_B",
                record.topic(),
                record.key(),
                parts[1],
                record.partition(),
                record.offset(),
                "analytics-group",
                "Analytics-Single-Consumer",
                String.format("Independent Group 'analytics-group' also received copy from Partition %d, Offset #%d (Pub/Sub Fan-Out).",
                        record.partition(), record.offset()),
                System.currentTimeMillis()
        ));
    }

    /**
     * OFFSET REPLAY: Seek all assigned demo-orders partitions back to Offset 0.
     * Retry and DLT listeners live in this bean too, so their partitions are filtered out
     * to avoid re-quarantining old poison pills.
     */
    public void replayFromBeginning() {
        String correlationId = "REPLAY-" + UUID.randomUUID().toString().substring(0, 4);
        telemetry.emit(new TraceEvent(
                correlationId,
                "REPLAY_TRIGGERED",
                KafkaConfig.MAIN_TOPIC,
                "ALL",
                "SEEK_TO_OFFSET_0",
                null,
                0L,
                "fulfillment-group & analytics-group",
                "ConsumerSeekCallback",
                "Rewinding consumer group offsets to 0 across all partitions! Historical messages will now re-stream.",
                System.currentTimeMillis()
        ));
        assignments.forEach((callback, topicPartitions) -> {
            List<TopicPartition> mainPartitions = topicPartitions.stream()
                    .filter(tp -> tp.topic().equals(KafkaConfig.MAIN_TOPIC))
                    .toList();
            if (!mainPartitions.isEmpty()) {
                callback.seekToBeginning(mainPartitions);
            }
        });
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        consumerCallback.set(callback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assigned, ConsumerSeekCallback callback) {
        ConsumerSeekCallback owner = consumerCallback.get() != null ? consumerCallback.get() : callback;
        assignments.computeIfAbsent(owner, k -> ConcurrentHashMap.newKeySet()).addAll(assigned.keySet());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        ConsumerSeekCallback owner = consumerCallback.get();
        if (owner != null && assignments.containsKey(owner)) {
            assignments.get(owner).removeAll(partitions);
        }
    }

    @Override
    public void unregisterSeekCallback() {
        ConsumerSeekCallback owner = consumerCallback.get();
        if (owner != null) {
            assignments.remove(owner);
        }
        consumerCallback.remove();
    }

    private String[] parsePayload(String raw) {
        int idx = raw.indexOf('|');
        if (idx > 0) {
            return new String[]{raw.substring(0, idx), raw.substring(idx + 1)};
        }
        return new String[]{"UNKNOWN", raw};
    }
}
