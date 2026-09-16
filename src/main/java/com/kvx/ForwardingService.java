package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Service
public class ForwardingService {

    private static final Logger log = LoggerFactory.getLogger(ForwardingService.class);

    private final RestClient restClient = RestClient.create();
    private final String selfUrl;

    public ForwardingService(@Value("${node.self}") String selfUrl) {
        this.selfUrl = selfUrl;
        log.info("This node's URL: {}", selfUrl);
    }

    public String getSelfUrl() { return selfUrl; }

    public boolean isSelf(String nodeUrl) {
        return nodeUrl != null && nodeUrl.equals(selfUrl);
    }

    /** Legacy put — kept for compatibility. */
    public ResponseEntity<String> forwardPut(String targetBaseUrl, String key, String value) {
        try {
            return restClient.put()
                    .uri(targetBaseUrl + "/internal/forward/" + key)
                    .body(value)
                    .retrieve()
                    .toEntity(String.class);
        } catch (Exception e) {
            return ResponseEntity.status(502).body("Forward failed: " + e.getMessage());
        }
    }

    /** Legacy delete — kept for compatibility. */
    public ResponseEntity<String> forwardDelete(String targetBaseUrl, String key) {
        try {
            return restClient.delete()
                    .uri(targetBaseUrl + "/internal/forward/" + key)
                    .retrieve()
                    .toEntity(String.class);
        } catch (Exception e) {
            return ResponseEntity.status(502).body("Forward failed: " + e.getMessage());
        }
    }

    /** Trace-aware PUT forward. */
    public ResponseEntity<TraceInfo> forwardPutWithTrace(String targetBaseUrl, String key, String value) {
        try {
            return restClient.put()
                    .uri(targetBaseUrl + "/internal/forward/" + key)
                    .body(value)
                    .retrieve()
                    .toEntity(TraceInfo.class);
        } catch (Exception e) {
            log.warn("Forward PUT {} to {} failed: {}", key, targetBaseUrl, e.getMessage());
            return ResponseEntity.status(502).build();
        }
    }

    /** Trace-aware GET forward. */
    public ResponseEntity<TraceInfo> forwardGetWithTrace(String targetBaseUrl, String key) {
        try {
            return restClient.get()
                    .uri(targetBaseUrl + "/internal/forward-get/" + key)
                    .retrieve()
                    .toEntity(TraceInfo.class);
        } catch (Exception e) {
            log.warn("Forward GET {} to {} failed: {}", key, targetBaseUrl, e.getMessage());
            return ResponseEntity.status(502).build();
        }
    }

    /** Trace-aware DELETE forward. */
    public ResponseEntity<TraceInfo> forwardDeleteWithTrace(String targetBaseUrl, String key) {
        try {
            return restClient.delete()
                    .uri(targetBaseUrl + "/internal/forward/" + key)
                    .retrieve()
                    .toEntity(TraceInfo.class);
        } catch (Exception e) {
            log.warn("Forward DELETE {} to {} failed: {}", key, targetBaseUrl, e.getMessage());
            return ResponseEntity.status(502).build();
        }
    }

    /** Replicate a versioned write to a peer. Returns true if peer acked. */
    public boolean replicatePut(String targetBaseUrl, String key, String value, long version) {
        try {
            restClient.put()
                    .uri(targetBaseUrl + "/internal/replicate/" + key + "?version=" + version)
                    .body(value)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (Exception e) {
            log.warn("Replication PUT {} to {} failed: {}", key, targetBaseUrl, e.getMessage());
            return false;
        }
    }

    public boolean replicateDelete(String targetBaseUrl, String key) {
        try {
            restClient.delete()
                    .uri(targetBaseUrl + "/internal/replicate/" + key)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (Exception e) {
            log.warn("Replication DELETE {} to {} failed: {}", key, targetBaseUrl, e.getMessage());
            return false;
        }
    }

    /** Fetch a VersionedValue from a peer (quorum reads). */
    public VersionedValue fetchVersioned(String targetBaseUrl, String key) {
        try {
            return restClient.get()
                    .uri(targetBaseUrl + "/internal/versioned/" + key)
                    .retrieve()
                    .body(VersionedValue.class);
        } catch (Exception e) {
            return null;
        }
    }

    /** Legacy getRaw — kept for compatibility. */
    public ResponseEntity<String> forwardGetRaw(String targetBaseUrl, String key) {
        try {
            return restClient.get()
                    .uri(targetBaseUrl + "/api/kv/" + key)
                    .retrieve()
                    .toEntity(String.class);
        } catch (HttpClientErrorException.NotFound e) {
            return ResponseEntity.status(404).body("Key not found");
        } catch (Exception e) {
            return ResponseEntity.status(502).body("Forward failed: " + e.getMessage());
        }
    }
}