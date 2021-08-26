package com.github.nexmark.kafka.queries;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import java.util.Properties;

public class StreamsUtils {
    public static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MsgPOJOSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, MsgPOJOSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MsgPOJOSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, MsgPOJOSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
