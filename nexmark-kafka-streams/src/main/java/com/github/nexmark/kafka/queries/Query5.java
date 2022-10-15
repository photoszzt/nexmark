package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AuctionIdCntMax;
import com.github.nexmark.kafka.model.AuctionIdCount;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.LongAndTime;
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

import java.io.IOException;
import java.io.FileInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class Query5 implements NexmarkQuery {
    // public CountAction<Event> input;
    public LatencyCountTransformerSupplier<AuctionIdCntMax, AuctionIdCntMax> lcts;
    public ArrayList<Long> topo1ProcLat;
    public ArrayList<Long> topo2ProcLat;
    public ArrayList<Long> bidsQueueTime;
    public ArrayList<Long> auctionBidsQueueTime;
    private static final String TOPO1PROC_TAG = "subG1ProcLat";
    private static final String TOPO2PROC_TAG = "subG2ProcLat";
    private static final String BIDSQT_TAG = "bidsQueueDelay";
    private static final String AUCBIDSQT_TAG = "auctionBidsQueueDelay";

    public Query5(String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q5_sink_ets", baseDir,
                new IdentityValueMapper<AuctionIdCntMax>());
        topo1ProcLat = new ArrayList<>(NUM_STATS);
        topo2ProcLat = new ArrayList<>(NUM_STATS);
        bidsQueueTime = new ArrayList<>(NUM_STATS);
        auctionBidsQueueTime = new ArrayList<>(NUM_STATS);
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile)
            throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        String bidsTp = prop.getProperty("bids.name");
        String bidsTpRepar = prop.getProperty("bids.reparName");
        int bidsTpPar = Integer.parseInt(prop.getProperty("bids.numPar"));
        NewTopic bidsRepar = new NewTopic(bidsTp, bidsTpPar, REPLICATION_FACTOR);

        String auctionBidsTp = prop.getProperty("auctionBids.name");
        String auctionBidsTpRepar = prop.getProperty("auctionBids.reparName");
        int auctionBidsTpPar = Integer.parseInt(prop.getProperty("auctionBids.numPar"));
        NewTopic auctionBidsRepar = new NewTopic(auctionBidsTp, auctionBidsTpPar, REPLICATION_FACTOR);

        List<NewTopic> nps = new ArrayList<>(3);
        nps.add(out);
        nps.add(bidsRepar);
        nps.add(auctionBidsRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        Serde<Event> eSerde;
        Serde<StartEndTime> seSerde;
        Serde<AuctionIdCntMax> aicmSerde;
        Serde<AuctionIdCount> aicSerde;
        Serde<LongAndTime> latSerde;

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

            JSONPOJOSerde<LongAndTime> latSerdeJSON = new JSONPOJOSerde<LongAndTime>();
            latSerdeJSON.setClass(LongAndTime.class);
            latSerde = latSerdeJSON;
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

            MsgpPOJOSerde<LongAndTime> latSerdeMsgp = new MsgpPOJOSerde<>();
            latSerdeMsgp.setClass(LongAndTime.class);
            latSerde = latSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);
        KStream<Long, Event> bid = inputs
                .filter((key, value) -> {
                    if (value != null) {
                        value.setStartProcTsNano(System.nanoTime());
                        value.setInjTsMs(Instant.now().toEpochMilli());
                        return value.etype == Event.EType.BID;
                    } else {
                        return false;
                    }
                })
                .selectKey((key, value) -> {
                    long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(topo1ProcLat, procLat, TOPO1PROC_TAG);
                    return value.bid.auction;
                });

        TimeWindows tws = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(20))
                .advanceBy(Duration.ofSeconds(2));
        WindowBytesStoreSupplier auctionBidsWSSupplier = Stores.inMemoryWindowStore("auctionBidsCountStore",
                Duration.ofMillis(tws.gracePeriodMs() + tws.size()), Duration.ofMillis(tws.size()), false);

        KStream<StartEndTime, AuctionIdCount> auctionBids = bid
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(bidsTpRepar)
                        .withNumberOfPartitions(bidsTpPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(bidsQueueTime, queueDelay, BIDSQT_TAG);
                    return value;
                })
                .groupByKey(Grouped.with(Serdes.Long(), eSerde))
                .windowedBy(tws)
                .aggregate(new Initializer<LongAndTime>() {
                    @Override
                    public LongAndTime apply() {
                        return new LongAndTime(0, 0);
                    }
                }, new Aggregator<Long, Event, LongAndTime>() {
                    @Override
                    public LongAndTime apply(Long key, Event value, LongAndTime aggregate) {
                        // System.out.println("key: " + key + " ts: " + value.bid.dateTime + " agg: " +
                        // aggregate);
                        LongAndTime lat = new LongAndTime(aggregate.val + 1, 0);
                        lat.startExecNano = value.startProcTsNano();
                        return lat;
                    }
                }, Named.as("auctionBidsCount"),
                        Materialized.<Long, LongAndTime>as(auctionBidsWSSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(latSerde)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>()))
                .toStream()
                .mapValues((key, value) -> {
                    AuctionIdCount aic = new AuctionIdCount(key.key(), value.val, 0);
                    aic.startExecNano = value.startExecNano;
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return aic;
                })
                .selectKey((key, value) -> {
                    StartEndTime se = new StartEndTime(key.window().start(), key.window().end(), 0);
                    long procLat = System.nanoTime() - value.startExecNano;
                    StreamsUtils.appendLat(topo2ProcLat, procLat, TOPO2PROC_TAG);
                    return se;
                })
                .repartition(Repartitioned.with(seSerde, aicSerde)
                        .withName(auctionBidsTpRepar)
                        .withNumberOfPartitions(auctionBidsTpPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(auctionBidsQueueTime, queueDelay, AUCBIDSQT_TAG);
                    return value;
                });

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");

        KTable<StartEndTime, LongAndTime> maxBids = auctionBids
                .groupByKey(Grouped.with(seSerde, aicSerde))
                .aggregate(() -> new LongAndTime(0, 0),
                        (key, value, aggregate) -> {
                            // System.out.println("start " + key.startTime + " end: " + key.endTime +
                            // " aucId: " + value.aucId + " count: " + value.count +
                            // " aggregate: " + aggregate);
                            if (value.count > aggregate.val) {
                                LongAndTime lat = new LongAndTime(value.count, 0);
                                lat.startExecNano = value.startProcTsNano();
                                return lat;
                            } else {
                                aggregate.startExecNano = value.startProcTsNano();
                                return aggregate;
                            }
                        }, Named.as("maxBidsAgg"),
                        Materialized.<StartEndTime, LongAndTime>as(maxBidsKV)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(seSerde)
                                .withValueSerde(latSerde));
        auctionBids
                .join(maxBids, (leftValue, rightValue) -> {
                    long startExecNano = 0;
                    if (leftValue.startProcTsNano() == 0) {
                        startExecNano = rightValue.startProcTsNano();
                    } else if (rightValue.startProcTsNano() == 0) {
                        startExecNano = leftValue.startProcTsNano();
                    } else {
                        startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
                    }
                    assert startExecNano != 0;
                    AuctionIdCntMax aicm = new AuctionIdCntMax(leftValue.aucId,
                            leftValue.count, (long) (rightValue.val));
                    aicm.startExecNano = startExecNano;
                    if (rightValue.startExecNano < aicm.startExecNano) {
                        aicm.startExecNano = rightValue.startExecNano;
                    }
                    return aicm;
                })
                .filter((key, value) -> value.count >= value.maxCnt)
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(seSerde, aicmSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q5");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q5");
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

    // @Override
    // public void waitForFinish() {
    // lcts.waitForFinish();
    // }

    @Override
    public void outputRemainingStats() {
        lcts.outputRemainingStats();
        StreamsUtils.printRemaining(topo1ProcLat, TOPO1PROC_TAG);
        StreamsUtils.printRemaining(topo2ProcLat, TOPO2PROC_TAG);
        StreamsUtils.printRemaining(bidsQueueTime, BIDSQT_TAG);
        StreamsUtils.printRemaining(auctionBidsQueueTime, AUCBIDSQT_TAG);
    }
    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
