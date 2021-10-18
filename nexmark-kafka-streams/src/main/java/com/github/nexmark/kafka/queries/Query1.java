package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Bid;
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

public class Query1 implements NexmarkQuery {
    public CountAction<String, Event> caInput;
    public CountAction<String, Event> caOutput;

    public Query1() {
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic np = new NewTopic("nexmark-q1", 1, (short) 3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));

        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), new JSONPOJOSerde<Event>() {
                })
                .withTimestampExtractor(new JSONTimestampExtractor()));
        caInput = new CountAction<String, Event>();
        caOutput = new CountAction<String, Event>();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<>();
        serde.setClass(Event.class);

        inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.BID)
                .mapValues(value -> {
                    Bid b = value.bid;
                    Event e = new Event(
                            new Bid(b.auction, b.bidder, (b.price * 89) / 100, b.channel, b.url, b.dateTime, b.extra));
                    return e;
                }).peek(caOutput).to("nexmark-q1-out", Produced.valueSerde(serde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q1");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "nexmark-q1-client");
        return props;
    }
}
