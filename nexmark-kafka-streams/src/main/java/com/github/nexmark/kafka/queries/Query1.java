package com.github.nexmark.kafka.queries;

import java.util.Properties;

import com.github.nexmark.kafka.model.Bid;
import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class Query1 implements NexmarkQuery {

    public Query1() {
    }

    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), new JSONPOJOSerde<Event>(){})
                        .withTimestampExtractor(new JSONTimestampExtractor()));
        final KStream<String, Event> q1Out = inputs.filter((key, value) -> {
            return value.type == Event.Type.BID;
        }).mapValues(value -> {
            Bid b = value.bid;
            Event e = new Event(
                    new Bid(b.auction, b.bidder, (b.price * 89) / 100, b.channel, b.url, b.dateTime, b.extra));
            return e;
        });
        q1Out.to("nexmark-q1", Produced.valueSerde(new JSONPOJOSerde<Event>() {
        }));
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q1");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "nexmark-q1-client");
        return props;
    }
}
