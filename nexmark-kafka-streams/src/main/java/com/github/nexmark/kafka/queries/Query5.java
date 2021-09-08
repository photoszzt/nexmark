package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AuctionIdCntMax;
import com.github.nexmark.kafka.model.AuctionIdCount;
import com.github.nexmark.kafka.model.StartEndTime;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TimeWindows;
import com.github.nexmark.kafka.model.Event;

import java.time.Duration;
import java.util.Properties;

public class Query5 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>() {
        };
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<Long, Event> bid = inputs.filter((key, value) -> value.type == Event.Type.BID)
                .map((key, value) -> KeyValue.pair(value.bid.auction, value));
        KStream<StartEndTime, AuctionIdCount> auctionBids = bid.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(2)))
                .count().toStream().map((key, value) -> {
                    final StartEndTime startEndTime = new StartEndTime(
                            key.window().start(),
                            key.window().end());
                    final AuctionIdCount val = new AuctionIdCount(
                            key.key(),
                            value
                    );
                    return KeyValue.pair(startEndTime, val);
                });

        KTable<StartEndTime, Long> maxBids = auctionBids.groupByKey().aggregate(() -> 0L,
                (key, value, aggregate) -> {
                    if (value.count > aggregate) {
                        return value.count;
                    } else {
                        return aggregate;
                    }
                }
        );
        KTable<StartEndTime, AuctionIdCntMax> q5 = auctionBids.toTable()
                .join(maxBids, (leftValue, rightValue) ->
                        new AuctionIdCntMax(leftValue.aucId, leftValue.count, (long) rightValue)
        ).filter((key, value) -> value.count >= value.maxCnt);
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q5");
        return props;
    }
}
