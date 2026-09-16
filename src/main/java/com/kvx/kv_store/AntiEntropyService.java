package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

@Service
public class AntiEntropyService {

    private static final Logger log = LoggerFactory.getLogger(AntiEntropyService.class);

    private final RestClient restClient = RestClient.create();
    private final KeyValueService keyValueService;
    private final HashRing hashRing;
    private final ForwardingService forwardingService;

    public AntiEntropyService(KeyValueService keyValueService,
                              HashRing hashRing,
                              ForwardingService forwardingService,
                              @Value("${node.self}") String selfUrl) {
        this.keyValueService = keyValueService;
        this.hashRing = hashRing;
        this.forwardingService = forwardingService;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 8000)
    public void syncWithPeers() {
        for (String peer : hashRing.getNodes()) {
            if (forwardingService.isSelf(peer)) continue;
            syncPeer(peer);
        }
    }

    @SuppressWarnings("unchecked")
    private void syncPeer(String peerBase) {
        Map<String, VersionedValue> myKeys = keyValueService.getAll();
        if (myKeys.isEmpty()) return;

        Map<String, VersionedValue> peerKeys = fetchPeerKeys(peerBase);

        for (Map.Entry<String, VersionedValue> entry : myKeys.entrySet()) {
            String key = entry.getKey();
            VersionedValue mine = entry.getValue();

            List<String> owners = hashRing.getReplicaNodes(key);
            if (!owners.contains(peerBase)) continue;

            VersionedValue theirs = peerKeys.get(key);
            if (theirs != null && theirs.getVersion() >= mine.getVersion()) continue;

            try {
                restClient.put()
                        .uri(peerBase + "/internal/replicate/" + key + "?version=" + mine.getVersion())
                        .body(mine.getValue())
                        .retrieve()
                        .toBodilessEntity();
                log.info("[ANTI-ENTROPY] Healed {} v{} -> {}", key, mine.getVersion(), peerBase);
            } catch (Exception e) {
                log.debug("[ANTI-ENTROPY] Cannot reach {}: {}", peerBase, e.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, VersionedValue> fetchPeerKeys(String peerBase) {
        try {
            ResponseEntity<Map> resp = restClient.get()
                    .uri(peerBase + "/api/kv/keys")
                    .retrieve()
                    .toEntity(Map.class);

            Map<String, Object> raw = resp.getBody();
            if (raw == null) return Map.of();

            // Convert LinkedHashMap -> VersionedValue
            Map<String, VersionedValue> result = new java.util.HashMap<>();
            for (Map.Entry<String, Object> e : raw.entrySet()) {
                Object v = e.getValue();
                if (v instanceof Map<?, ?> m) {
                    String value = String.valueOf(m.get("value"));
                    long version = Long.parseLong(String.valueOf(m.get("version")));
                    result.put(e.getKey(), new VersionedValue(value, version));
                }
            }
            return result;
        } catch (Exception e) {
            return Map.of();
        }
    }
}