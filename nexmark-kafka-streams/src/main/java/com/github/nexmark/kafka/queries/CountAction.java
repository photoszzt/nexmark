package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.kstream.ForeachAction;

public class CountAction <K, V> implements ForeachAction<K, V> {
    long processedRecords = 0;

    @Override
    public void apply(K k, V v) {
        processedRecords += 1;
    }

    public long GetProcessedRecords() {
        return processedRecords;
    }
}
