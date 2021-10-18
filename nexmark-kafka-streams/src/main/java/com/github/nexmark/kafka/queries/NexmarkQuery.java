package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Map;
import java.util.Properties;

public interface NexmarkQuery {
    StreamsBuilder getStreamBuilder(String bootstrapServer);
    Properties getProperties(String bootstrapServer);
    Map<String, CountAction> getCountActionMap();
}
