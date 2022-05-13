package com.github.nexmark.kafka.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.streams.kstream.ForeachAction;

public class LatencyCount<K, V extends TimestampFromValue<V>> implements ForeachAction<K, TimestampFromValue<V>> {
    List<Long> latencies = new ArrayList<>();
    AtomicBoolean afterWarmup = new AtomicBoolean(false);

    @Override
    public void apply(K key, TimestampFromValue<V> value) {
        if (afterWarmup.get()) {
            long ts = value.extract();
            long lat = System.currentTimeMillis() - ts;
            latencies.add(lat);
        }
    }

    public List<Long> GetLatencies() {
        return latencies;
    }

    public void SetAfterWarmup() {
        afterWarmup.set(true);
    }
}
