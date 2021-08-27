package com.github.nexmark.kafka.queries;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.github.nexmark.kafka.model.Bid;
import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class Query1 implements NexmarkQuery {

    public Query1() {}

    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), new MsgPOJOSerde<>()));
        KStream<String, Event> q1Out = inputs.filter((key, value) -> value.type == Event.Type.BID)
                .mapValues(value -> {
                    Bid b = value.bid;
                    return new Event(new Bid(b.auction, b.bidder, (b.price * 89) / 100,
                            b.channel, b.url, b.dateTime, b.extra));
                });
        q1Out.to("nexmark-q1");
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q1");
        return props;
    }
}
