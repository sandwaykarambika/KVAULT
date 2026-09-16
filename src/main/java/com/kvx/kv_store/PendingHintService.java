package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class PendingHintService {

    private static final Logger log = LoggerFactory.getLogger(PendingHintService.class);

    /** targetNodeUrl -> list of hints for that node */
    private final Map<String, CopyOnWriteArrayList<PendingHint>> hints = new ConcurrentHashMap<>();

    private final ForwardingService forwardingService;

    public PendingHintService(ForwardingService forwardingService) {
        this.forwardingService = forwardingService;
    }

    /** Queue a hint when a replica is unreachable. */
    public void addHint(String targetNode, String key, String value, long version) {
        hints.computeIfAbsent(targetNode, k -> new CopyOnWriteArrayList<>())
                .add(new PendingHint(key, value, version));
        log.warn("[HINT] Queued {} v{} for {}", key, version, targetNode);
    }

    /** Total pending hints (used by dashboard). */
    public int totalHints() {
        return hints.values().stream().mapToInt(List::size).sum();
    }

    /** Per-node hint counts (used by dashboard). */
    public Map<String, Integer> hintsByNode() {
        Map<String, Integer> result = new ConcurrentHashMap<>();
        hints.forEach((node, list) -> result.put(node, list.size()));
        return result;
    }

    /**
     * Every 5 seconds, try to deliver hints to their targets.
     * If delivery succeeds, remove the hint; if the node is still down, keep it.
     */
    @Scheduled(fixedDelay = 5000, initialDelay = 10000)
    public void deliverHints() {
        if (hints.isEmpty()) return;

        for (Map.Entry<String, CopyOnWriteArrayList<PendingHint>> entry : hints.entrySet()) {
            String targetNode = entry.getKey();
            CopyOnWriteArrayList<PendingHint> list = entry.getValue();
            if (list.isEmpty()) continue;

            List<PendingHint> delivered = new ArrayList<>();

            for (PendingHint hint : list) {
                boolean ok = forwardingService.replicatePut(
                        targetNode, hint.getKey(), hint.getValue(), hint.getVersion());
                if (ok) {
                    delivered.add(hint);
                    log.info("[HINT-DELIVERED] {} v{} → {}", hint.getKey(), hint.getVersion(), targetNode);
                }
            }

            list.removeAll(delivered);
            if (list.isEmpty()) hints.remove(targetNode);
        }
    }
}