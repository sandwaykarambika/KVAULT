package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/internal")
public class InternalController {

    private static final Logger log = LoggerFactory.getLogger(InternalController.class);

    private final KeyValueService keyValueService;
    private final HashRing hashRing;
    private final ForwardingService forwardingService;
    private final NodeHealth nodeHealth;
    private final PendingHintService pendingHintService;

    @Value("${consistency.w:2}")
    private int writeQuorum;

    @Value("${node.self}")
    private String selfUrl;

    public InternalController(KeyValueService keyValueService,
                              HashRing hashRing,
                              ForwardingService forwardingService,
                              NodeHealth nodeHealth,
                              PendingHintService pendingHintService) {
        this.keyValueService = keyValueService;
        this.hashRing = hashRing;
        this.forwardingService = forwardingService;
        this.nodeHealth = nodeHealth;
        this.pendingHintService = pendingHintService;
    }

    // ---------------- FORWARD PUT (returns TraceInfo) ----------------
    @PutMapping("/forward/{key}")
    public ResponseEntity<TraceInfo> forwardPut(@PathVariable String key, @RequestBody String value) {
        TraceInfo trace = new TraceInfo("PUT", key);
        trace.setHash(hashRing.hashOf(key));
        trace.setReceivedBy(selfUrl);
        trace.setForwarded(true);
        trace.setPrimary(selfUrl);
        trace.setValue(value);

        if (!nodeHealth.isHealthy()) {
            trace.setSuccess(false);
            trace.setMessage("Node is in simulated outage");
            return ResponseEntity.status(503).body(trace);
        }

        log.info("[FORWARD PUT] {} = {}", key, value);

        VersionedValue vv = keyValueService.put(key, value);
        long version = vv.getVersion();

        List<String> replicas = hashRing.getReplicaNodes(key);
        trace.setReplicas(replicas);
        trace.setRequiredQuorum(writeQuorum);

        int acks = 1;
        int hints = 0;

        for (String node : replicas) {
            if (forwardingService.isSelf(node)) continue;
            boolean ok = forwardingService.replicatePut(node, key, value, version);
            if (ok) acks++;
            else {
                pendingHintService.addHint(node, key, value, version);
                acks++;
                hints++;
            }
        }

        trace.setAcks(acks);
        trace.setHints(hints);
        trace.setSuccess(acks >= writeQuorum);
        trace.setMessage(acks >= writeQuorum
            ? "Quorum achieved (" + acks + "/" + writeQuorum + ")"
            : "Quorum failed (" + acks + "/" + writeQuorum + ")");

        return ResponseEntity.ok(trace);
    }

    // ---------------- FORWARD GET (returns TraceInfo) ----------------
    @GetMapping("/forward-get/{key}")
    public ResponseEntity<TraceInfo> forwardGet(@PathVariable String key) {
        TraceInfo trace = new TraceInfo("GET", key);
        trace.setHash(hashRing.hashOf(key));
        trace.setReceivedBy(selfUrl);
        trace.setForwarded(true);
        trace.setPrimary(selfUrl);
        trace.setReplicas(hashRing.getReplicaNodes(key));
        trace.setRequiredQuorum(1);

        if (!nodeHealth.isHealthy()) {
            trace.setSuccess(false);
            trace.setMessage("Node is in simulated outage");
            return ResponseEntity.status(503).body(trace);
        }

        VersionedValue vv = keyValueService.get(key);
        if (vv == null) {
            trace.setSuccess(false);
            trace.setMessage("Key not found");
            return ResponseEntity.ok(trace);
        }

        trace.setValue(vv.getValue());
        trace.setAcks(1);
        trace.setSuccess(true);
        trace.setMessage("Read from primary, version v" + vv.getVersion());
        return ResponseEntity.ok(trace);
    }

    // ---------------- FORWARD DELETE (returns TraceInfo) ----------------
    @DeleteMapping("/forward/{key}")
    public ResponseEntity<TraceInfo> forwardDelete(@PathVariable String key) {
        TraceInfo trace = new TraceInfo("DELETE", key);
        trace.setHash(hashRing.hashOf(key));
        trace.setReceivedBy(selfUrl);
        trace.setForwarded(true);
        trace.setPrimary(selfUrl);
        trace.setRequiredQuorum(writeQuorum);

        if (!nodeHealth.isHealthy()) {
            trace.setSuccess(false);
            trace.setMessage("Node is in simulated outage");
            return ResponseEntity.status(503).body(trace);
        }

        log.info("[FORWARD DELETE] {}", key);

        boolean deleted = keyValueService.delete(key);
        if (!deleted) {
            trace.setSuccess(false);
            trace.setMessage("Key not found");
            return ResponseEntity.ok(trace);
        }

        List<String> replicas = hashRing.getReplicaNodes(key);
        trace.setReplicas(replicas);

        int acks = 1;
        for (String node : replicas) {
            if (forwardingService.isSelf(node)) continue;
            if (forwardingService.replicateDelete(node, key)) acks++;
        }

        trace.setAcks(acks);
        trace.setSuccess(acks >= writeQuorum);
        trace.setMessage("Deleted (" + acks + "/" + writeQuorum + " acks)");
        return ResponseEntity.ok(trace);
    }

    // ---------------- REPLICATION ----------------
    @PutMapping("/replicate/{key}")
    public ResponseEntity<String> replicatePut(@PathVariable String key,
                                                @RequestBody String value,
                                                @RequestParam(value = "version", required = false) Long version) {
        if (!nodeHealth.isHealthy()) {
            return ResponseEntity.status(503).body("Node is in simulated outage");
        }
        long v = version != null ? version : System.currentTimeMillis();
        keyValueService.putVersioned(key, value, v);
        log.info("[REPLICA PUT] {} v{}", key, v);
        return ResponseEntity.ok("Replicated: " + key);
    }

    @DeleteMapping("/replicate/{key}")
    public ResponseEntity<String> replicateDelete(@PathVariable String key) {
        if (!nodeHealth.isHealthy()) {
            return ResponseEntity.status(503).body("Node is in simulated outage");
        }
        log.info("[REPLICA DELETE] {}", key);
        keyValueService.delete(key);
        return ResponseEntity.ok("Replicated delete: " + key);
    }

    @GetMapping("/versioned/{key}")
    public ResponseEntity<VersionedValue> getVersioned(@PathVariable String key) {
        if (!nodeHealth.isHealthy()) {
            return ResponseEntity.status(503).build();
        }
        VersionedValue vv = keyValueService.get(key);
        if (vv == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(vv);
    }

    @GetMapping("/hints")
    public ResponseEntity<java.util.Map<String, Object>> hintsInfo() {
        java.util.Map<String, Object> result = new java.util.HashMap<>();
        result.put("total", pendingHintService.totalHints());
        result.put("byNode", pendingHintService.hintsByNode());
        return ResponseEntity.ok(result);
    }
}