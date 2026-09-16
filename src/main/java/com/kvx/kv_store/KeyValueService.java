package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KeyValueService {

    private static final Logger log = LoggerFactory.getLogger(KeyValueService.class);

    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();
    private final Path walFile;
    private final Object fileLock = new Object();

    public KeyValueService(@Value("${node.id}") String nodeId) throws IOException {
        Path dataDir = Paths.get("data");
        Files.createDirectories(dataDir);
        this.walFile = dataDir.resolve(nodeId + ".wal");
        replay();
    }

    private void replay() throws IOException {
        if (!Files.exists(walFile)) {
            log.info("No WAL file at {} — starting fresh", walFile);
            return;
        }
        int ops = 0;
        for (String line : Files.readAllLines(walFile)) {
            if (line.isBlank()) continue;
            // Format: PUT key value version   OR   DELETE key   OR legacy: PUT key value
            if (line.startsWith("PUT ")) {
                String[] parts = line.split(" ", 4);
                if (parts.length >= 3) {
                    String key = parts[1];
                    String value = parts[2];
                    long version = parts.length == 4 ? parseLongSafe(parts[3]) : System.currentTimeMillis();
                    store.put(key, new VersionedValue(value, version));
                    ops++;
                }
            } else if (line.startsWith("DELETE ")) {
                String[] parts = line.split(" ", 2);
                if (parts.length == 2) {
                    store.remove(parts[1]);
                    ops++;
                }
            }
        }
        log.info("Replayed {} ops from WAL ({}) — {} keys restored", ops, walFile, store.size());
    }

    private long parseLongSafe(String s) {
        try { return Long.parseLong(s); } catch (Exception e) { return System.currentTimeMillis(); }
    }

    /** Client-facing put: auto-assign version from local clock. */
    public VersionedValue put(String key, String value) {
        long version = System.currentTimeMillis();
        VersionedValue vv = new VersionedValue(value, version);
        store.put(key, vv);
        append("PUT " + key + " " + value + " " + version);
        return vv;
    }

    /** Replication put: preserve incoming version; only accept if newer. */
    public VersionedValue putVersioned(String key, String value, long version) {
        VersionedValue incoming = new VersionedValue(value, version);
        VersionedValue existing = store.get(key);
        if (existing == null || existing.getVersion() <= version) {
            store.put(key, incoming);
            append("PUT " + key + " " + value + " " + version);
            return incoming;
        }
        return existing;
    }

    public VersionedValue get(String key) {
        return store.get(key);
    }

    public boolean delete(String key) {
        boolean removed = store.remove(key) != null;
        if (removed) append("DELETE " + key);
        return removed;
    }

    public Map<String, VersionedValue> getAll() {
        return new HashMap<>(store);
    }

    private void append(String line) {
        synchronized (fileLock) {
            try {
                Files.writeString(walFile, line + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                log.warn("WAL append failed: {}", e.getMessage());
            }
        }
    }
}