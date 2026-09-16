package com.kvx.kv_store;

import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class NodeHealth {
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    public boolean isHealthy() { return healthy.get(); }
    public void setHealthy(boolean value) { healthy.set(value); }
    public void simulateOutage() { healthy.set(false); }
    public void recover() { healthy.set(true); }
}