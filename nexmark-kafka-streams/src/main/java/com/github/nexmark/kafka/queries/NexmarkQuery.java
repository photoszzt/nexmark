package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

public interface NexmarkQuery {
    public StreamsBuilder getStreamBuilder();
    public Properties getProperties();
}
