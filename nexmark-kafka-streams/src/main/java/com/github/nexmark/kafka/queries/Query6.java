package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Query6 implements NexmarkQuery {

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) {
        return null;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q6");
        return props;
    }
}
