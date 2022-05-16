package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public interface NexmarkQuery {
    StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException;
    Properties getProperties(String bootstrapServer, int duration, int flushms);
    long getInputCount();
    void setAfterWarmup();
    List<Long> getRecordE2ELatency();
}
