package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AucIdCategory;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.SumAndCount;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class Query4 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCountTransformerSupplier<Double> lcts;
    
    public Query4() {
        input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Event> eSerde;
        Serde<AucIdCategory> aicSerde;
        Serde<AuctionBid> abSerde;
        Serde<SumAndCount> scSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<AucIdCategory> aicSerdeJSON = new JSONPOJOSerde<>();
            aicSerdeJSON.setClass(AucIdCategory.class);
            aicSerde = aicSerdeJSON;

            JSONPOJOSerde<AuctionBid> abSerdeJSON = new JSONPOJOSerde<>();
            abSerdeJSON.setClass(AuctionBid.class);
            abSerde = abSerdeJSON;

            JSONPOJOSerde<SumAndCount> scSerdeJSON = new JSONPOJOSerde<>();
            scSerdeJSON.setClass(SumAndCount.class);
            scSerde = scSerdeJSON;

        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<AucIdCategory> aicSerdeMsgp = new MsgpPOJOSerde<>();
            aicSerdeMsgp.setClass(AucIdCategory.class);
            aicSerde = aicSerdeMsgp;

            MsgpPOJOSerde<AuctionBid> abSerdeMsgp = new MsgpPOJOSerde<>();
            abSerdeMsgp.setClass(AuctionBid.class);
            abSerde = abSerdeMsgp;

            MsgpPOJOSerde<SumAndCount> scSerdeMsgp = new MsgpPOJOSerde<>();
            scSerdeMsgp.setClass(SumAndCount.class);
            scSerde = scSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde)
                        .withTimestampExtractor(new EventTimestampExtractor())).peek(input);

        KeyValueBytesStoreSupplier bidKVSupplier = Stores.inMemoryKeyValueStore("bidTab");
        KTable<Long, Event> bid = inputs
                .filter((key, value) -> value.etype == Event.EType.BID)
                .selectKey((key, value) -> value.bid.auction)
                .toTable(Named.as("bidTabNode"),
                        Materialized.<Long, Event>as(bidKVSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        KeyValueBytesStoreSupplier auctionKVSupplier = Stores.inMemoryKeyValueStore("auctionTab");
        KTable<Long, Event> auction = inputs
                .filter((key, value) -> value.etype == Event.EType.AUCTION)
                .selectKey((key, value) -> value.newAuction.id)
                .toTable(Named.as("auctionTabNode"),
                        Materialized.<Long, Event>as(auctionKVSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");
        KTable<AucIdCategory, Long> maxBids = auction
                .join(bid, (leftValue, rightValue) -> new AuctionBid(rightValue.bid.dateTime,
                        leftValue.newAuction.dateTimeMs, leftValue.newAuction.expiresMs,
                        rightValue.bid.price, leftValue.newAuction.category))
                .filter((key, value) -> value.bidDateTimeMs >= value.aucDateTimeMs
                        && value.bidDateTimeMs <= value.aucExpiresMs)
                .toStream()
                .selectKey((key, value) -> new AucIdCategory(key, value.aucCategory))
                .groupByKey(Grouped.with(aicSerde, abSerde))
                .aggregate(() -> 0L, (key, value, aggregate) -> {
                    if (value.bidPrice > aggregate) {
                        return value.bidPrice;
                    } else {
                        return aggregate;
                    }
                }, Named.as("maxBidPrice"), Materialized.<AucIdCategory, Long>as(maxBidsKV)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(aicSerde)
                        .withValueSerde(Serdes.Long()));

        KeyValueBytesStoreSupplier sumCountKV = Stores.inMemoryKeyValueStore("sumCountKVStore");
        maxBids.toStream()
                .selectKey((key, value) -> key.category)
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
                .aggregate(() -> new SumAndCount(0, 0),
                        (key, value, aggregate) -> new SumAndCount(aggregate.sum + value, aggregate.count + 1),
                        Named.as("sumCount"), Materialized.<Long, SumAndCount>as(sumCountKV)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(scSerde))
                .mapValues((key, value) -> (double) value.sum / (double) value.count)
                .toStream()
                .transformValues(lcts, Named.as("latency-measure"))
                .to("q4-out", Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer, int duration, int flushms) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer, duration, flushms);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q4");
        return props;
    }

    @Override
    public long getInputCount() {
        return input.GetProcessedRecords();
    }

    @Override
    public void setAfterWarmup() {
        lcts.SetAfterWarmup();
    }

    @Override
    public List<Long> getRecordE2ELatency() {
        return lcts.GetLatency();
    }
}
