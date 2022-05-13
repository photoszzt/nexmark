package com.github.nexmark.kafka.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LatencyHolder {
    List<Long> latencies;
    AtomicBoolean afterWarmup;

    public LatencyHolder() {
        latencies = new ArrayList<>();
        afterWarmup = new AtomicBoolean(false);
    }

    public void SetAfterWarmup() {
        afterWarmup.set(true);
    }

    public void AppendLatency(long lat) {
        if (afterWarmup.get()) {
            latencies.add(lat);
        }
    }

    public List<Long> GetLatency() {
        return latencies;
    }
}
