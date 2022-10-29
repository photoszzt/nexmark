package com.github.nexmark.kafka.queries;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.streams.kstream.ValueMapper;

public class CountAction<V extends StartProcTs> implements ValueMapper<V, V> {
    AtomicLong processedRecords = new AtomicLong(0);

    public long GetProcessedRecords() {
        return processedRecords.get();
    }

    @Override
    public V apply(final V v) {
        v.setStartProcTsNano(System.nanoTime());
        processedRecords.incrementAndGet();
        return v;
    }
}
