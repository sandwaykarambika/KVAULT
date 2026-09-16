package com.kvx.kv_store;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class HealthController {

    @Value("${node.id}")
    private String nodeId;

    private final NodeHealth nodeHealth;

    public HealthController(NodeHealth nodeHealth) {
        this.nodeHealth = nodeHealth;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> body = new HashMap<>();
        body.put("service", "distributed-kv-store");
        body.put("nodeId", nodeId);
        body.put("status", nodeHealth.isHealthy() ? "UP" : "DOWN");
        HttpStatus status = nodeHealth.isHealthy() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(status).body(body);
    }
}