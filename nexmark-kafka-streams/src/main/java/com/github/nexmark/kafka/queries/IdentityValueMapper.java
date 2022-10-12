package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.kstream.ValueMapper;

public class IdentityValueMapper<V extends StartProcTs> implements ValueMapper<V, V> {
    @Override
    public V apply(V value) {
        return value;
    }
}