package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public interface NexmarkQuery {
    StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException;
    Properties getProperties(String bootstrapServer);
    Map<String, CountAction> getCountActionMap();
}
