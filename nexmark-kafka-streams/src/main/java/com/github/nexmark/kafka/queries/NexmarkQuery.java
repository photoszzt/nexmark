package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

public interface NexmarkQuery {
    StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException;
    Properties getProperties(String bootstrapServer, int duration);
    long getInputCount();
    void setAfterWarmup();
    List<Long> getRecordE2ELatency();
}
