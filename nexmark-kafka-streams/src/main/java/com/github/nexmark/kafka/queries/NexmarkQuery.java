package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.util.Properties;

public interface NexmarkQuery {
    StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException;
    Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms, boolean disableCache);
    Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms, boolean disableCache);
    void setAfterWarmup();
    void printCount();
    void outputRemainingStats();
}
