package com.github.nexmark.kafka.queries;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.streams.kstream.ForeachAction;

public class CountAction <K, V> implements ForeachAction<K, V> {
    AtomicLong processedRecords = new AtomicLong(0);

    @Override
    public void apply(K k, V v) {
        // System.out.println("################## get event k: " + k + " v: " + v);
        processedRecords.incrementAndGet();
    }

    public long GetProcessedRecords() {
        return processedRecords.get();
    }
}
