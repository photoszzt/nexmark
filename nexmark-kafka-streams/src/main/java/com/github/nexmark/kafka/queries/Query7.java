package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PriceTime;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class Query7 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>() {
        };
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<Long, Event> bid = inputs.filter((key, value) -> value.type == Event.Type.BID)
                .selectKey((key, value) -> value.bid.price);
        KTable<Windowed<Long>, PriceTime> maxBid = bid.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(
                        () -> new PriceTime(0, Instant.MIN),
                        (key, value, aggregate) -> {
                            if (value.bid.price > aggregate.price) {
                                return new PriceTime(value.bid.price,
                                        value.bid.dateTime);
                            } else {
                                return aggregate;
                            }
                        });
        /*
        bid.join(maxBid, (leftValue, rightValue) ->
                new BidAndMax(leftValue.bid.auction, leftValue.bid.price,
                        leftValue.bid.bidder, leftValue.bid.dateTime,
                        leftValue.bid.extra, rightValue.dateTime)
        );

         */
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q7");
        return props;
    }
}
