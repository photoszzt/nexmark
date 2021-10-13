package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Properties;

public class Query2 implements NexmarkQuery {
    public CountAction<String, Event> caInput;
    public CountAction<String, Event> caOutput;

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic np = new NewTopic("nexmark-q2", 1, (short) 3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));
        StreamsBuilder builder = new StreamsBuilder();

        caInput = new CountAction<>();
        caOutput = new CountAction<>();
        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), new JSONPOJOSerde<Event>() {
                        })
                        .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<String, Event> q2Out = inputs.peek(caInput).filter((key, value) -> value.type == Event.Type.BID)
                .filter((key, value) -> value.bid.auction % 123 == 0);
        q2Out.peek(caOutput).to("nexmark-q2", Produced.valueSerde(new JSONPOJOSerde<Event>() {
        }));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q2");
        return props;
    }
}
