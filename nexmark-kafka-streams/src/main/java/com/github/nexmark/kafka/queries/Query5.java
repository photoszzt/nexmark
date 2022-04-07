package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AuctionIdCntMax;
import com.github.nexmark.kafka.model.AuctionIdCount;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.StartEndTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

public class Query5 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query5() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) {
        int numPartition = 5;
        short replicationFactor = 3;
        List<NewTopic> nps = new ArrayList<>();
        NewTopic out = new NewTopic("nexmark-q5-out", numPartition, replicationFactor);
        NewTopic bidsRepar = new NewTopic("nexmark-q5-bids-repartition", numPartition, replicationFactor);
        NewTopic auctionBidsRepar = new NewTopic("nexmark-q5-auctionBids-repartition", numPartition, replicationFactor);
        nps.add(out);
        nps.add(bidsRepar);
        nps.add(auctionBidsRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        CountAction<String, Event> caInput = new CountAction<String, Event>();
        CountAction<StartEndTime, AuctionIdCntMax> caOutput = new CountAction<StartEndTime, AuctionIdCntMax>();
        caMap.put("caInput", caInput);
        caMap.put("caOutput", caOutput);

        Serde<Event> eSerde;
        Serde<StartEndTime> seSerde;
        Serde<AuctionIdCntMax> aicmSerde;
        Serde<AuctionIdCount> aicSerde;

        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<StartEndTime> seSerdeJSON = new JSONPOJOSerde<StartEndTime>();
            seSerdeJSON.setClass(StartEndTime.class);
            seSerde = seSerdeJSON;

            JSONPOJOSerde<AuctionIdCount> aicSerdeJSON = new JSONPOJOSerde<AuctionIdCount>();
            aicSerdeJSON.setClass(AuctionIdCount.class);
            aicSerde = aicSerdeJSON;

            JSONPOJOSerde<AuctionIdCntMax> aicmSerdeJSON = new JSONPOJOSerde<AuctionIdCntMax>();
            aicmSerdeJSON.setClass(AuctionIdCntMax.class);
            aicmSerde = aicmSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<StartEndTime> seSerdeMsgp = new MsgpPOJOSerde<>();
            seSerdeMsgp.setClass(StartEndTime.class);
            seSerde = seSerdeMsgp;

            MsgpPOJOSerde<AuctionIdCount> aicSerdeMsgp = new MsgpPOJOSerde<>();
            aicSerdeMsgp.setClass(AuctionIdCount.class);
            aicSerde = aicSerdeMsgp;

            MsgpPOJOSerde<AuctionIdCntMax> aicmSerdeMsgp = new MsgpPOJOSerde<>();
            aicmSerdeMsgp.setClass(AuctionIdCntMax.class);
            aicmSerde = aicmSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));
        KStream<Long, Event> bid = inputs.peek(caInput).filter((key, value) -> value.etype == Event.EType.BID)
                .selectKey((key, value) -> value.bid.auction);

        TimeWindows ts = TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(2));
        WindowBytesStoreSupplier auctionBidsWSSupplier = Stores.inMemoryWindowStore("auctionBidsCountStore",
                Duration.ofMillis(ts.gracePeriodMs() + ts.size()), Duration.ofMillis(ts.size()), true);

        int numberOfPartitions = 5;
        KStream<StartEndTime, AuctionIdCount> auctionBids = bid
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName("bids-repartition")
                        .withNumberOfPartitions(numberOfPartitions))
                .groupByKey(Grouped.with(Serdes.Long(), eSerde))
                .windowedBy(ts)
                .count(Named.as("auctionBidsCount"),
                        Materialized.<Long, Long>as(auctionBidsWSSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(Serdes.Long()))
                .toStream()
                .mapValues((key, value) -> new AuctionIdCount(key.key(), value))
                .selectKey((key, value) -> new StartEndTime(key.window().start(), key.window().end()))
                .repartition(Repartitioned.with(seSerde, aicSerde)
                        .withName("auctionBids-repartition")
                        .withNumberOfPartitions(numberOfPartitions));

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");
        
        KTable<StartEndTime, Long> maxBids = auctionBids
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
