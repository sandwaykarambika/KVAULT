package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kv")
public class KeyValueController {

    private static final Logger log = LoggerFactory.getLogger(KeyValueController.class);

    private final KeyValueService keyValueService;
    private final HashRing hashRing;
    private final ForwardingService forwardingService;
    private final PendingHintService pendingHintService;

    @Value("${consistency.w:2}")
    private int writeQuorum;

    @Value("${consistency.r:1}")
    private int readQuorum;

    @Value("${node.self}")
    private String selfUrl;

    public KeyValueController(KeyValueService keyValueService,
                              HashRing hashRing,
                              ForwardingService forwardingService,
                              PendingHintService pendingHintService) {
        this.keyValueService = keyValueService;
        this.hashRing = hashRing;
        this.forwardingService = forwardingService;
        this.pendingHintService = pendingHintService;
    }

    // ================== PUT ==================
    @PutMapping("/{key}")
    public ResponseEntity<TraceInfo> put(@PathVariable String key, @RequestBody String value) {
        String primary = hashRing.getPrimaryNode(key);

        if (forwardingService.isSelf(primary)) {
            // I am primary — store + replicate
            TraceInfo trace = storeAsPrimary(key, value, "PUT");
            trace.setReceivedBy(selfUrl);
            trace.setForwarded(false);
            trace.setPrimary(primary);
            return ResponseEntity.ok(trace);
        }

        // Not primary — forward
        log.info("[FORWARD PUT] {} = {}  → {}", key, value, primary);
        ResponseEntity<TraceInfo> resp = forwardingService.forwardPutWithTrace(primary, key, value);

        if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
            TraceInfo trace = resp.getBody();
            trace.setReceivedBy(selfUrl);
            trace.setForwarded(true);
            return ResponseEntity.ok(trace);
        }

        // Primary down — failover
        log.warn("[FAILOVER] Primary {} down for {} — taking over", primary, key);
        TraceInfo failover = storeAsPrimary(key, value, "PUT");
        failover.setReceivedBy(selfUrl);
        failover.setForwarded(true);
        failover.setPrimary(primary);
        if (keyValueService.get(key) != null) {
            pendingHintService.addHint(primary, key, value, keyValueService.get(key).getVersion());
        }
        failover.setMessage("Failover: hint queued for down primary");
        return ResponseEntity.ok(failover);
    }

    private TraceInfo storeAsPrimary(String key, String value, String op) {
        VersionedValue vv = keyValueService.put(key, value);
        long version = vv.getVersion();

        List<String> replicas = hashRing.getReplicaNodes(key);
        List<String> ackedNodes = new ArrayList<>();
        ackedNodes.add(selfUrl);

        int acks = 1;
        int hints = 0;

        for (String node : replicas) {
            if (forwardingService.isSelf(node)) continue;
            boolean ok = forwardingService.replicatePut(node, key, value, version);
            if (ok) {
                acks++;
                ackedNodes.add(node);
            } else {
                pendingHintService.addHint(node, key, value, version);
                acks++;
                hints++;
                ackedNodes.add(node + " (hint)");
            }
        }

        TraceInfo trace = new TraceInfo(op, key);
        trace.setValue(value);
        trace.setHash(hashRing.hashOf(key));
        trace.setReplicas(replicas);
        trace.setAcks(acks);
        trace.setRequiredQuorum(writeQuorum);
        trace.setHints(hints);
        trace.setSuccess(acks >= writeQuorum);
        trace.setMessage(acks >= writeQuorum
            ? "Quorum achieved (" + acks + "/" + writeQuorum + ")"
            : "Quorum failed (" + acks + "/" + writeQuorum + ")");
        return trace;
    }

    // ================== GET ==================
    @GetMapping("/{key}")
    public ResponseEntity<TraceInfo> get(@PathVariable String key) {
        String primary = hashRing.getPrimaryNode(key);

        if (forwardingService.isSelf(primary)) {
            TraceInfo trace = quorumRead(key);
            trace.setReceivedBy(selfUrl);
            trace.setForwarded(false);
            trace.setPrimary(primary);
            return ResponseEntity.ok(trace);
        }

        ResponseEntity<TraceInfo> resp = forwardingService.forwardGetWithTrace(primary, key);
        if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
            TraceInfo trace = resp.getBody();
            trace.setReceivedBy(selfUrl);
            trace.setForwarded(true);
            return ResponseEntity.ok(trace);
        }

        // Primary down — read locally
        TraceInfo trace = quorumRead(key);
        trace.setReceivedBy(selfUrl);
        trace.setForwarded(true);
        trace.setPrimary(primary);
        return ResponseEntity.ok(trace);
    }

    private TraceInfo quorumRead(String key) {
        List<String> replicas = hashRing.getReplicaNodes(key);
        List<VersionedValue> reads = new ArrayList<>();

        VersionedValue local = keyValueService.get(key);
        if (local != null) reads.add(local);

        for (String node : replicas) {
            if (reads.size() >= readQuorum) break;
            if (forwardingService.isSelf(node)) continue;
            VersionedValue peer = forwardingService.fetchVersioned(node, key);
            if (peer != null) reads.add(peer);
        }

        TraceInfo trace = new TraceInfo("GET", key);
        trace.setHash(hashRing.hashOf(key));
        trace.setReplicas(replicas);
        trace.setRequiredQuorum(readQuorum);

        if (reads.isEmpty()) {
            trace.setSuccess(false);
            trace.setMessage("Key not found");
            trace.setValue(null);
            return trace;
        }

        VersionedValue winner = reads.get(0);
        for (VersionedValue vv : reads) {
            if (vv.getVersion() > winner.getVersion()) winner = vv;
        }

        trace.setValue(winner.getValue());
        trace.setAcks(reads.size());
        trace.setSuccess(true);
        trace.setMessage("Read from " + reads.size() + " replica(s), version v" + winner.getVersion());
        return trace;
    }

    // ================== DELETE ==================
    @DeleteMapping("/{key}")
    public ResponseEntity<TraceInfo> delete(@PathVariable String key) {
        String primary = hashRing.getPrimaryNode(key);

        if (forwardingService.isSelf(primary)) {
            return doDelete(key, false);
        }

        ResponseEntity<TraceInfo> resp = forwardingService.forwardDeleteWithTrace(primary, key);
        if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
            TraceInfo trace = resp.getBody();
            trace.setReceivedBy(selfUrl);
            trace.setForwarded(true);
            return ResponseEntity.ok(trace);
        }

        // Failover
        return doDelete(key, true);
    }

    private ResponseEntity<TraceInfo> doDelete(String key, boolean forwarded) {
        boolean deleted = keyValueService.delete(key);
        TraceInfo trace = new TraceInfo("DELETE", key);
        trace.setHash(hashRing.hashOf(key));
        trace.setReplicas(hashRing.getReplicaNodes(key));
        trace.setRequiredQuorum(writeQuorum);
        trace.setReceivedBy(selfUrl);
        trace.setForwarded(forwarded);
        trace.setPrimary(hashRing.getPrimaryNode(key));

        if (!deleted) {
            trace.setSuccess(false);
            trace.setMessage("Key not found");
            return ResponseEntity.ok(trace);
        }

        for (String node : hashRing.getReplicaNodes(key)) {
            if (forwardingService.isSelf(node)) continue;
            forwardingService.replicateDelete(node, key);
        }

        trace.setSuccess(true);
        trace.setAcks(2);
        trace.setMessage("Deleted from primary + replicas");
        return ResponseEntity.ok(trace);
    }

    @GetMapping("/keys")
    public ResponseEntity<Map<String, VersionedValue>> keys() {
        return ResponseEntity.ok(keyValueService.getAll());
    }
}