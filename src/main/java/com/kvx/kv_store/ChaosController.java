package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/chaos")
public class ChaosController {

    private static final Logger log = LoggerFactory.getLogger(ChaosController.class);

    private final NodeHealth nodeHealth;

    @Value("${node.id}")
    private String nodeId;

    public ChaosController(NodeHealth nodeHealth) {
        this.nodeHealth = nodeHealth;
    }

    @PostMapping("/outage")
    public ResponseEntity<String> triggerOutage() {
        nodeHealth.simulateOutage();
        log.warn("[CHAOS] {} simulating outage", nodeId);
        return ResponseEntity.ok("Outage simulated on " + nodeId);
    }

    @PostMapping("/recover")
    public ResponseEntity<String> triggerRecover() {
        nodeHealth.recover();
        log.info("[CHAOS] {} recovered", nodeId);
        return ResponseEntity.ok("Recovered " + nodeId);
    }
}