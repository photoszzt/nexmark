package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class Query2 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Event> inputs = builder.stream("nexmark-input",
                Consumed.with(Serdes.String(), new JSONPOJOSerde<Event>()));
        KStream<String, Event> q2Out = inputs.filter((key, value) -> value.type == Event.Type.BID)
                .filter((key, value) -> value.bid.auction % 123 == 0);
        q2Out.to("nexmark-q2", Produced.valueSerde(new JSONPOJOSerde<Event>() {
        }));
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q2");
        return props;
    }
}
