package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AucIdCategory;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.DoubleAndTime;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.LongAndTime;
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
import java.time.Instant;

import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class Query4 implements NexmarkQuery {
    // public CountAction<Event> input;
    public LatencyCountTransformerSupplier<DoubleAndTime, Double> lcts;
    private ArrayList<Long> aucProcLat;
    private ArrayList<Long> bidProcLat;
    private ArrayList<Long> aucQueueTime;
    private ArrayList<Long> bidQueueTime;
    private ArrayList<Long> aucBidsQueueTime;
    private ArrayList<Long> maxBidsQueueTime;
    public ArrayList<Long> topo2ProcLat;
    public ArrayList<Long> topo3ProcLat;
    private static final Duration auctionDurationUpperS = Duration.ofSeconds(1800);

    public Query4(String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q4_sink_ets", baseDir,
                new ValueMapper<DoubleAndTime, Double>() {
                    @Override
                    public Double apply(DoubleAndTime value) {
                        return value.avg;
                    }
                });
        aucProcLat = new ArrayList<>(NUM_STATS);
        bidProcLat = new ArrayList<>(NUM_STATS);
        aucQueueTime = new ArrayList<>(NUM_STATS);
        bidQueueTime = new ArrayList<>(NUM_STATS);
        aucBidsQueueTime = new ArrayList<>(NUM_STATS);
        maxBidsQueueTime = new ArrayList<>(NUM_STATS);
        topo2ProcLat = new ArrayList<>(NUM_STATS);
        topo3ProcLat = new ArrayList<>(NUM_STATS);
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
        Serde<LongAndTime> ltSerde;
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

            JSONPOJOSerde<LongAndTime> ltSerdeJSON = new JSONPOJOSerde<>();
            ltSerdeJSON.setClass(LongAndTime.class);
            ltSerde = ltSerdeJSON;

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

            MsgpPOJOSerde<LongAndTime> ltSerdeMsgp = new MsgpPOJOSerde<>();
            ltSerdeMsgp.setClass(LongAndTime.class);
            ltSerde = ltSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde)
                        .withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);
        Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
                .branch((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return value.etype == Event.EType.BID;
                }, Branched.as("bids"))
                .branch((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return value.etype == Event.EType.AUCTION;
                }, Branched.as("auctions"))
                .noDefaultBranch();

        KStream<Long, Event> bidsByAucID = ksMap.get("Branch-bids")
                .selectKey((key, value) -> {
                    long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(bidProcLat, procLat, "subGBid_proc");
                    return value.bid.auction;
                })
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(bidsByAucIDTpRepar)
                        .withNumberOfPartitions(bidsByAucIDTpNumPar))
                .mapValues((key, value) -> {
                    assert value.injTsMs() != 0;
                    value.setStartProcTsNano(System.nanoTime());
                    assert value.startProcTsNano() != 0;
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(bidQueueTime, queueDelay, "bidQueueDelay");
                    return value;
                }, Named.as("bidMapValues"));

        KStream<Long, Event> aucsByID = ksMap.get("Branch-auctions")
                .selectKey((key, value) -> {
                    long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(aucProcLat, procLat, "subGAuc_proc");
                    return value.newAuction.id;
                })
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(aucsByIDTpRepar)
                        .withNumberOfPartitions(aucsByIDTpNumPar))
                .mapValues((key, value) -> {
                    assert value.injTsMs() != 0;
                    value.setStartProcTsNano(System.nanoTime());
                    assert value.startProcTsNano() != 0;
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(aucQueueTime, queueDelay, "aucQueueDelay");
                    return value;
                }, Named.as("aucMapValues"));

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");

        JoinWindows jw = JoinWindows.ofTimeDifferenceWithNoGrace(auctionDurationUpperS);
        WindowBytesStoreSupplier aucsByIDStoreSupplier = Stores.inMemoryWindowStore(
                "aucsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);
        WindowBytesStoreSupplier bidsByAucIDStoreSupplier = Stores.inMemoryWindowStore(
                "bidsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);

        KTable<AucIdCategory, LongAndTime> maxBids = aucsByID.join(bidsByAucID, (leftValue, rightValue) -> {
            long startExecNano = 0;
            if (leftValue.startProcTsNano() == 0) {
                startExecNano = rightValue.startProcTsNano();
            } else if (rightValue.startProcTsNano() == 0) {
                startExecNano = leftValue.startProcTsNano();
            } else {
                startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
            }
            // System.out.println("leftStart: " + leftValue.startProcTsNano() + " rightStart: " + rightValue.startProcTsNano() + " startExecNano: " + startExecNano);
            assert startExecNano != 0;
            AuctionBid ab = new AuctionBid(rightValue.bid.dateTime,
                    leftValue.newAuction.dateTime, leftValue.newAuction.expires,
                    rightValue.bid.price, leftValue.newAuction.category,
                    leftValue.newAuction.seller, 0);
            ab.setStartProcTsNano(startExecNano);
            return ab;
        }, jw, StreamJoined.<Long, Event, Event>with(aucsByIDStoreSupplier, bidsByAucIDStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(eSerde)
                .withLoggingEnabled(new HashMap<>()))
                .filter((key, value) -> value.bidDateTimeMs >= value.aucDateTimeMs
                        && value.bidDateTimeMs <= value.aucExpiresMs)
                .filter((key, value) -> {
                    // System.out.println("filuterNull, key: " + key + " value: " + value);
                    if (value != null) {
                        value.setInjTsMs(Instant.now().toEpochMilli());
                        return true;
                    } else {
                        return false;
                    }
                })
                .selectKey(new KeyValueMapper<Long, AuctionBid, AucIdCategory>() {
                    @Override
                    public AucIdCategory apply(Long key, AuctionBid value) {
                        assert value.startProcTsNano() != 0;
                        long lat = System.nanoTime() - value.startProcTsNano();
                        StreamsUtils.appendLat(topo2ProcLat, lat, "subG2_proc");
                        return new AucIdCategory(key, value.aucCategory);
                    }
                })
                .repartition(Repartitioned.with(aicSerde, abSerde)
                        .withName(aucBidsTpRepar).withNumberOfPartitions(aucBidsTpNumPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(aucBidsQueueTime, queueDelay, "aucBidsQueueDelay");
                    return value;
                }, Named.as("topo3-beg"))
                .groupByKey()
                .aggregate(new Initializer<LongAndTime>() {
                    @Override
                    public LongAndTime apply() {
                        return new LongAndTime(null, 0);
                    }
                }, new Aggregator<AucIdCategory, AuctionBid, LongAndTime>() {
                    @Override
                    public LongAndTime apply(AucIdCategory key, AuctionBid value, LongAndTime aggregate) {
                        assert value.startProcTsNano() != 0;
                        if (aggregate.val == null) {
                            LongAndTime lt = new LongAndTime(value.bidPrice, 0);
                            lt.startExecNano = value.startProcTsNano();
                            return lt;
                        }
                        if (value.bidPrice > aggregate.val) {
                            LongAndTime lt = new LongAndTime(value.bidPrice, 0);
                            lt.startExecNano = value.startProcTsNano();
                            return lt;
                        } else {
                            aggregate.startExecNano = value.startProcTsNano();
                            return aggregate;
                        }
                    }
                }, Named.as("maxBidPrice"), Materialized.<AucIdCategory, LongAndTime>as(maxBidsKV)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(aicSerde)
                        .withValueSerde(ltSerde));

        KeyValueBytesStoreSupplier sumCountKV = Stores.inMemoryKeyValueStore("sumCountKVStore");
        maxBids.groupBy(new KeyValueMapper<AucIdCategory, LongAndTime, KeyValue<Long, LongAndTime>>() {
            @Override
            public KeyValue<Long, LongAndTime> apply(AucIdCategory key, LongAndTime value) {
                // System.out.println("max, key: " + key + " value: " + value);
                long procLat = System.nanoTime() - value.startExecNano;
                StreamsUtils.appendLat(topo3ProcLat, procLat, "subG3_proc");
                value.injTsMs = Instant.now().toEpochMilli();
                return new KeyValue<Long, LongAndTime>(key.category, value);
            }
        }, Grouped.with(Serdes.Long(), ltSerde).withName(maxBidsGroupByTab))
                .aggregate(() -> {
                    SumAndCount s = new SumAndCount(0, 0);
                    s.startExecNano = System.nanoTime();
                    return s;
                }, new Aggregator<Long, LongAndTime, SumAndCount>() {
                    @Override
                    public SumAndCount apply(Long key, LongAndTime value, SumAndCount aggregate) {
                        if (value != null) {
                            long queueDelay = Instant.now().toEpochMilli() - value.injTsMs;
                            StreamsUtils.appendLat(maxBidsQueueTime, queueDelay, "maxBidsQueueDelay");
                            SumAndCount sc = new SumAndCount(aggregate.sum + value.val, aggregate.count + 1);
                            if (aggregate.sum == 0) {
                                sc.startExecNano = aggregate.startExecNano;
                            } else {
                                sc.startExecNano = System.nanoTime();
                            }
                            return sc;
                        } else {
                            aggregate.startExecNano = System.nanoTime();
                            return aggregate;
                        }
                    }
                }, new Aggregator<Long, LongAndTime, SumAndCount>() {
                    @Override
                    public SumAndCount apply(Long key, LongAndTime value, SumAndCount aggregate) {
                        if (value != null) {
                            SumAndCount sc = new SumAndCount(aggregate.sum - value.val, aggregate.count - 1);
                            if (aggregate.sum == 0) {
                                sc.startExecNano = aggregate.startExecNano;
                            } else {
                                sc.startExecNano = System.nanoTime();
                            }
                            return sc;
                        } else {
                            aggregate.startExecNano = System.nanoTime();
                            return aggregate;
                        }
                    }
                }, Named.as("sumCount"),
                        Materialized.<Long, SumAndCount>as(sumCountKV)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(scSerde)
                                .withCachingEnabled() // match the behavior of golang sys
                                .withLoggingEnabled(new HashMap<>()))
                .mapValues((key, value) -> {
                    DoubleAndTime d = new DoubleAndTime((double) value.sum / (double) value.count);
                    d.startExecNano = value.startExecNano;
                    return d;
                })
                .toStream()
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q4");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q4");
        return props;
    }

    // @Override
    // public long getInputCount() {
    // return input.GetProcessedRecords();
    // }

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
        StreamsUtils.printRemaining(aucProcLat, "subGAuc_proc");
        StreamsUtils.printRemaining(bidProcLat, "subGBid_proc");
        StreamsUtils.printRemaining(aucQueueTime, "aucQueueDelay");
        StreamsUtils.printRemaining(bidQueueTime, "bidQueueDelay");
        StreamsUtils.printRemaining(aucBidsQueueTime, "aucBidsQueueDelay");
        StreamsUtils.printRemaining(maxBidsQueueTime, "maxBidsQueueDelay");
        StreamsUtils.printRemaining(topo2ProcLat, "subG2_proc");
        StreamsUtils.printRemaining(topo3ProcLat, "subG3_proc");
    }
    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
