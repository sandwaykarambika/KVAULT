package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

@Component
public class HashRing {

    private static final Logger log = LoggerFactory.getLogger(HashRing.class);

    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final List<String> nodes;
    private final int replicationFactor;

    public HashRing(
            @Value("${cluster.nodes}") String nodesCsv,
            @Value("${cluster.virtual-nodes:16}") int virtualNodes,
            @Value("${cluster.replication-factor:2}") int replicationFactor) {

        this.nodes = Arrays.stream(nodesCsv.split(",")).map(String::trim).toList();
        this.replicationFactor = replicationFactor;

        for (String node : nodes) {
            for (int i = 0; i < virtualNodes; i++) {
                long hash = hash(node + "#" + i);
                ring.put(hash, node);
            }
        }

        log.info("HashRing initialized with {} physical nodes, {} virtual nodes each, replication factor {}",
                nodes.size(), virtualNodes, replicationFactor);
        log.info("Cluster nodes: {}", nodes);
    }

    /** Returns the primary node responsible for a key. */
    public String getPrimaryNode(String key) {
        if (ring.isEmpty()) throw new IllegalStateException("Hash ring is empty");
        long hash = hash(key);
        var entry = ring.ceilingEntry(hash);
        if (entry == null) entry = ring.firstEntry(); // wrap around
        return entry.getValue();
    }

    /**
     * Returns the list of nodes that should hold a copy of the key.
     * Walks clockwise on the ring, picking distinct physical nodes
     * until we've reached the replication factor.
     */
    public List<String> getReplicaNodes(String key) {
        if (ring.isEmpty()) return List.of();
        long hash = hash(key);
        Set<String> result = new LinkedHashSet<>();

        // Iterate clockwise starting from the key's position
        var tail = ring.tailMap(hash, true);
        for (String node : tail.values()) {
            result.add(node);
            if (result.size() >= replicationFactor) return new ArrayList<>(result);
        }
        // Wrap around to the beginning
        for (String node : ring.headMap(hash, false).values()) {
            result.add(node);
            if (result.size() >= replicationFactor) return new ArrayList<>(result);
        }
        // Fallback: if replication factor > number of nodes, return all
        return new ArrayList<>(result);
    }

    /** Murmur-ish deterministic hash using SHA-256 truncated to 64 bits. */
    private static long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Used by the dashboard to visualize which nodes own which slots. */
    public TreeMap<Long, String> getRing() {
        return ring;
    }

    public List<String> getNodes() {
        return nodes;
    }
        /** Exposes the hash for tracing/visualization. */
    public String hashOf(String key) {
        long h = hash(key);
        return "0x" + Long.toHexString(h);
    }
}