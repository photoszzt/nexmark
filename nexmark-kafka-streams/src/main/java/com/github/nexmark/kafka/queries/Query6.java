package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Query6 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query6() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde) {
        return null;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q6");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
