package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AucIdCategory;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.SumAndCount;
import org.apache.kafka.clients.admin.NewTopic;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;

import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query4 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCountTransformerSupplier<Double> lcts;
    private static final Duration auctionDurationUpperS = Duration.ofSeconds(1800);

    public Query4(String baseDir) {
        input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q4_sink_ets", baseDir);
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException {

        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        String bidsByAucIDTp = prop.getProperty("bidsByAucIDTp.name");
        String bidsByAucIDTpRepar = prop.getProperty("bidsByAucIDTp.reparName");
        int bidsByAucIDTpNumPar = Integer.parseInt(prop.getProperty("bidsByAucIDTp.numPar"));
        NewTopic bidsByAucIDPar = new NewTopic(bidsByAucIDTp, bidsByAucIDTpNumPar, REPLICATION_FACTOR);

        String aucsByIDTp = prop.getProperty("aucsByIDTp.name");
        int aucsByIDTpNumPar = Integer.parseInt(prop.getProperty("aucsByIDTp.numPar"));
        String aucsByIDTpRepar = prop.getProperty("aucsByIDTp.reparName");
        NewTopic aucsByIDPar = new NewTopic(aucsByIDTp, aucsByIDTpNumPar, REPLICATION_FACTOR);

        String aucBidsTp = prop.getProperty("aucBidsTp.name");
        String aucBidsTpRepar = prop.getProperty("aucBidsTp.reparName");
        int aucBidsTpNumPar = Integer.parseInt(prop.getProperty("aucBidsTp.numPar"));
        NewTopic aucBidsPar = new NewTopic(aucBidsTp, aucBidsTpNumPar, REPLICATION_FACTOR);

        String maxBidsGroupByTab = prop.getProperty("maxBidsGroupByTab");
        String maxBidsTp = prop.getProperty("maxBidsTp.name");
        int maxBidsTpNumPar = Integer.parseInt(prop.getProperty("maxBidsTp.numPar"));
        NewTopic maxBidsTpPar = new NewTopic(maxBidsTp, maxBidsTpNumPar, REPLICATION_FACTOR);

        List<NewTopic> nps = new ArrayList<NewTopic>(5);
        nps.add(out);
        nps.add(bidsByAucIDPar);
        nps.add(aucsByIDPar);
        nps.add(aucBidsPar);
        nps.add(maxBidsTpPar);
        StreamsUtils.createTopic(bootstrapServer, nps);

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
                        .withTimestampExtractor(new EventTimestampExtractor()))
                .peek(input);
        Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
                .branch((key, value) -> value.etype == Event.EType.BID, Branched.as("bids"))
                .branch((key, value) -> value.etype == Event.EType.AUCTION, Branched.as("auctions"))
                .noDefaultBranch();

        KStream<Long, Event> bidsByAucID = ksMap.get("Branch-bids").selectKey((key, value) -> value.bid.auction)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(bidsByAucIDTpRepar)
                        .withNumberOfPartitions(bidsByAucIDTpNumPar));

        KStream<Long, Event> aucsByID = ksMap.get("Branch-auctions")
                .selectKey((key, value) -> value.newAuction.id)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(aucsByIDTpRepar)
                        .withNumberOfPartitions(aucsByIDTpNumPar));

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");

        JoinWindows jw = JoinWindows.ofTimeDifferenceWithNoGrace(auctionDurationUpperS);
        WindowBytesStoreSupplier aucsByIDStoreSupplier = Stores.inMemoryWindowStore(
                "aucsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);
        WindowBytesStoreSupplier bidsByAucIDStoreSupplier = Stores.inMemoryWindowStore(
                "bidsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);

        KTable<AucIdCategory, Long> maxBids = aucsByID.join(bidsByAucID, (leftValue, rightValue) -> {
            return new AuctionBid(rightValue.bid.dateTime,
                    leftValue.newAuction.dateTime, leftValue.newAuction.expires,
                    rightValue.bid.price, leftValue.newAuction.category, 
                    leftValue.newAuction.seller);
        }, jw, StreamJoined.<Long, Event, Event>with(aucsByIDStoreSupplier, bidsByAucIDStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(eSerde)
                .withLoggingEnabled(new HashMap<>()))
                .filter((key, value) -> value.bidDateTimeMs >= value.aucDateTimeMs
                        && value.bidDateTimeMs <= value.aucExpiresMs)
                .filter((key, value) -> {
                    // System.out.println("filuterNull, key: " + key + " value: " + value);
                    return value != null;
                })
                .selectKey(new KeyValueMapper<Long, AuctionBid, AucIdCategory>() {
                    @Override
                    public AucIdCategory apply(Long key, AuctionBid value) {
                        // System.out.println("selectKey, key: " + key + " value: " + value);
                        return new AucIdCategory(key, value.aucCategory);
                    }
                })
                .repartition(Repartitioned.with(aicSerde, abSerde)
                        .withName(aucBidsTpRepar).withNumberOfPartitions(aucBidsTpNumPar))
                .groupByKey()
                .aggregate(new Initializer<Long>() {
                    @Override
                    public Long apply() {
                        // TODO Auto-generated method stub
                        return null;
                    }
                }, new Aggregator<AucIdCategory, AuctionBid, Long>() {
                    @Override
                    public Long apply(AucIdCategory key, AuctionBid value, Long aggregate) {
                        if (aggregate == null) {
                            return value.bidPrice;
                        }
                        if (value.bidPrice > aggregate) {
                            return value.bidPrice;
                        } else {
                            return aggregate;
                        }
                    }
                }, Named.as("maxBidPrice"), Materialized.<AucIdCategory, Long>as(maxBidsKV)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(aicSerde)
                        .withValueSerde(Serdes.Long()));

        KeyValueBytesStoreSupplier sumCountKV = Stores.inMemoryKeyValueStore("sumCountKVStore");
        maxBids.groupBy(new KeyValueMapper<AucIdCategory, Long, KeyValue<Long, Long>>() {
            @Override
            public KeyValue<Long, Long> apply(AucIdCategory key, Long value) {
                // TODO Auto-generated method stub
                // System.out.println("max, key: " + key + " value: " + value);
                return new KeyValue<Long, Long>(key.category, value);
            }
        }, Grouped.with(Serdes.Long(), Serdes.Long()).withName(maxBidsGroupByTab))
                .aggregate(() -> new SumAndCount(0, 0), new Aggregator<Long, Long, SumAndCount>() {
                    @Override
                    public SumAndCount apply(Long key, Long value, SumAndCount aggregate) {
                        if (value != null) {
                            return new SumAndCount(aggregate.sum + value, aggregate.count + 1);
                        } else {
                            return aggregate;
                        }
                    }
                }, new Aggregator<Long, Long, SumAndCount>() {
                    @Override
                    public SumAndCount apply(Long key, Long value, SumAndCount aggregate) {
                        if (value != null) {
                            return new SumAndCount(aggregate.sum - value, aggregate.count - 1);
                        } else {
                            return aggregate;
                        }
                    }
                }, Named.as("sumCount"),
                        Materialized.<Long, SumAndCount>as(sumCountKV)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(scSerde)
                                .withCachingEnabled() // match the behavior of golang sys
                                .withLoggingEnabled(new HashMap<>()))
                .mapValues((key, value) -> (double) value.sum / (double) value.count)
                .toStream()
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms, boolean disableCache) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q4");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms, boolean disableCache) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q4");
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
    public void printCount() {
        lcts.printCount();
    }

    public void waitForFinish() {
        lcts.waitForFinish();
    }

    @Override
    public void outputRemainingStats() {
        lcts.outputRemainingStats();
    }
    // @Override
    // public void printRemainingStats() {
    //     lcts.printRemainingStats();
    // }
}
