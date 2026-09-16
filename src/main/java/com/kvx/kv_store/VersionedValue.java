package com.kvx.kv_store;

public class VersionedValue {
    private String value;
    private long version;   // monotonic timestamp

    public VersionedValue() {}

    public VersionedValue(String value, long version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }

    @Override
    public String toString() {
        return value + "@" + version;
    }
}