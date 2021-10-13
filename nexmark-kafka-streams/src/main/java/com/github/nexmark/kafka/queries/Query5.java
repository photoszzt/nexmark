package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.*;
import com.sun.tools.javadoc.Start;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Query5 implements NexmarkQuery {
    public CountAction<String, Event> caInput;
    public CountAction<StartEndTime, AuctionIdCntMax> caOutput;

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {

        NewTopic np = new NewTopic("nexmark-q5", 1, (short)3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));

        caInput = new CountAction();
        caOutput = new CountAction();

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>() {
        };
        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<Long, Event> bid = inputs.peek(caInput).filter((key, value) -> value.type == Event.Type.BID)
                .selectKey((key, value) -> value.bid.auction);
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
        auctionBids
                .join(maxBids, (leftValue, rightValue) ->
                        new AuctionIdCntMax(leftValue.aucId, leftValue.count, (long) rightValue))
                .filter((key, value) -> value.count >= value.maxCnt)
                .peek(caOutput)
                .to("nexmark-q5", Produced.with(new JSONPOJOSerde<StartEndTime>() {}, new JSONPOJOSerde<AuctionIdCntMax>(){}));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q5");
        return props;
    }
}
