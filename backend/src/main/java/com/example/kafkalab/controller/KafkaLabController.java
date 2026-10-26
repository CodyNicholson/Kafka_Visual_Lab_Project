package com.example.kafkalab.controller;

import com.example.kafkalab.service.KafkaDemoService;
import com.example.kafkalab.service.TelemetryService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaLabController {

    private final KafkaDemoService kafkaDemoService;
    private final TelemetryService telemetryService;

    public KafkaLabController(KafkaDemoService kafkaDemoService, TelemetryService telemetryService) {
        this.kafkaDemoService = kafkaDemoService;
        this.telemetryService = telemetryService;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents() {
        return telemetryService.registerClient();
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendMessage(
            @RequestParam(required = false) String key,
            @RequestParam String payload,
            @RequestParam(defaultValue = "false") boolean roundRobin
    ) {
        String normalizedKey = (key == null || key.isBlank()) ? null : key;
        kafkaDemoService.sendDemoMessage(normalizedKey, payload, roundRobin);
        return ResponseEntity.ok(Map.of("status", "dispatched"));
    }

    @PostMapping("/replay")
    public ResponseEntity<Map<String, String>> replayOffsets() {
        kafkaDemoService.replayFromBeginning();
        return ResponseEntity.ok(Map.of("status", "replaying_from_offset_0"));
    }
}
