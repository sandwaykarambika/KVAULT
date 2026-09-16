package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Arrays;
import java.util.List;

@Service
public class ReplicationService {

    private static final Logger log = LoggerFactory.getLogger(ReplicationService.class);

    private final RestClient restClient = RestClient.create();
    private final List<String> peers;

    public ReplicationService(@Value("${node.peers:}") String peersCsv) {
        this.peers = peersCsv.isBlank()
            ? List.of()
            : Arrays.stream(peersCsv.split(",")).map(String::trim).toList();
        log.info("Replication peers configured: {}", this.peers);
    }

    public void replicatePut(String key, String value) {
        for (String peer : peers) {
            try {
                restClient.put()
                    .uri(peer + "/internal/replicate/" + key)
                    .body(value)
                    .retrieve()
                    .toBodilessEntity();
                log.info("Replicated PUT {} -> {}", key, peer);
            } catch (Exception e) {
                log.warn("Failed to replicate PUT {} to {}: {}", key, peer, e.getMessage());
            }
        }
    }

    public void replicateDelete(String key) {
        for (String peer : peers) {
            try {
                restClient.delete()
                    .uri(peer + "/internal/replicate/" + key)
                    .retrieve()
                    .toBodilessEntity();
                log.info("Replicated DELETE {} -> {}", key, peer);
            } catch (Exception e) {
                log.warn("Failed to replicate DELETE {} to {}: {}", key, peer, e.getMessage());
            }
        }
    }
}