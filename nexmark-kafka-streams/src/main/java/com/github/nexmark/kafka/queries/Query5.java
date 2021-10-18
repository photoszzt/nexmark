package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AuctionIdCntMax;
import com.github.nexmark.kafka.model.AuctionIdCount;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.StartEndTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Query5 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query5() {
        caMap = new HashMap<>();
        caMap.put("caInput", new CountAction<String, Event>());
        caMap.put("caOutput", new CountAction<StartEndTime, AuctionIdCntMax>());
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {

        NewTopic np = new NewTopic("nexmark-q5", 1, (short) 3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));

        CountAction<String, Event> caInput = caMap.get("caInput");
        CountAction<StartEndTime, AuctionIdCntMax> caOutput = caMap.get("caOutput");

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>();
        serde.setClass(Event.class);

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<Long, Event> bid = inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.BID)
                .selectKey((key, value) -> value.bid.auction);

        TimeWindows ts = TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(2));
        WindowBytesStoreSupplier auctionBidsWSSupplier = Stores.inMemoryWindowStore("auctionBidsCountStore",
                Duration.ofMillis(ts.gracePeriodMs() + ts.size()), Duration.ofMillis(ts.size()), true);

        int numberOfPartitions = 5;
        KStream<StartEndTime, AuctionIdCount> auctionBids = bid
                .repartition(Repartitioned.with(Serdes.Long(), serde)
                        .withName("auctionBids-repartition-node")
                        .withNumberOfPartitions(numberOfPartitions))
                .groupByKey(Grouped.with(Serdes.Long(), serde))
                .windowedBy(ts)
                .count(Named.as("auctionBidsCount"),
                        Materialized.<Long, Long>as(auctionBidsWSSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(Serdes.Long()))
                .toStream()
                .map((key, value) -> {
                    final StartEndTime startEndTime = new StartEndTime(
                            key.window().start(),
                            key.window().end());
                    final AuctionIdCount val = new AuctionIdCount(
                            key.key(),
                            value
                    );
                    return KeyValue.pair(startEndTime, val);
                });

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");
        JSONPOJOSerde<StartEndTime> seSerde = new JSONPOJOSerde<StartEndTime>();
        seSerde.setClass(StartEndTime.class);
        JSONPOJOSerde<AuctionIdCount> aicSerde = new JSONPOJOSerde<AuctionIdCount>();
        aicSerde.setClass(AuctionIdCount.class);
        JSONPOJOSerde<AuctionIdCntMax> aicmSerde = new JSONPOJOSerde<AuctionIdCntMax>();
        aicmSerde.setClass(AuctionIdCntMax.class);

        KTable<StartEndTime, Long> maxBids = auctionBids
                .repartition(Repartitioned.with(seSerde, aicSerde)
                        .withName("maxBids-repartition-node")
                        .withNumberOfPartitions(numberOfPartitions))
                .groupByKey(Grouped.with(seSerde, aicSerde))
                .aggregate(() -> 0L,
                        (key, value, aggregate) -> {
                            if (value.count > aggregate) {
                                return value.count;
                            } else {
                                return aggregate;
                            }
                        }, Named.as("maxBidsAgg"),
                        Materialized.<StartEndTime, Long>as(maxBidsKV)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(seSerde)
                                .withValueSerde(Serdes.Long())
                );
        auctionBids
                .join(maxBids, (leftValue, rightValue) ->
                        new AuctionIdCntMax(leftValue.aucId, leftValue.count, (long) rightValue))
                .filter((key, value) -> value.count >= value.maxCnt)
                .peek(caOutput)
                .to("nexmark-q5-out", Produced.with(seSerde, aicmSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q5");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
