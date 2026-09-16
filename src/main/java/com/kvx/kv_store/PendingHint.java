package com.kvx.kv_store;

public class PendingHint {
    private final String key;
    private final String value;
    private final long version;
    private final long createdAt;

    public PendingHint(String key, String value, long version) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.createdAt = System.currentTimeMillis();
    }

    public String getKey() { return key; }
    public String getValue() { return value; }
    public long getVersion() { return version; }
    public long getCreatedAt() { return createdAt; }
}