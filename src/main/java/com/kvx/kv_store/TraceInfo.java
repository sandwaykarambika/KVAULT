package com.kvx.kv_store;

import java.util.ArrayList;
import java.util.List;

public class TraceInfo {
    private String key;
    private String operation;       // PUT, GET, DELETE
    private String value;
    private String hash;
    private String receivedBy;      // which node got the request first
    private String primary;         // which node owns the key
    private boolean forwarded;
    private List<String> replicas = new ArrayList<>();
    private int acks;
    private int requiredQuorum;
    private int hints;
    private boolean success;
    private String message;
    private long timestamp = System.currentTimeMillis();

    public TraceInfo() {}
    public TraceInfo(String operation, String key) {
        this.operation = operation;
        this.key = key;
    }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    public String getHash() { return hash; }
    public void setHash(String hash) { this.hash = hash; }

    public String getReceivedBy() { return receivedBy; }
    public void setReceivedBy(String receivedBy) { this.receivedBy = receivedBy; }

    public String getPrimary() { return primary; }
    public void setPrimary(String primary) { this.primary = primary; }

    public boolean isForwarded() { return forwarded; }
    public void setForwarded(boolean forwarded) { this.forwarded = forwarded; }

    public List<String> getReplicas() { return replicas; }
    public void setReplicas(List<String> replicas) { this.replicas = replicas; }

    public int getAcks() { return acks; }
    public void setAcks(int acks) { this.acks = acks; }

    public int getRequiredQuorum() { return requiredQuorum; }
    public void setRequiredQuorum(int requiredQuorum) { this.requiredQuorum = requiredQuorum; }

    public int getHints() { return hints; }
    public void setHints(int hints) { this.hints = hints; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}