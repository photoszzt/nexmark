package com.github.nexmark.kafka.queries;

public interface TimestampFromValue<V> {
    long extract();
}
