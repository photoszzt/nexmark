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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.KTable;
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
    private final LatencyCountTransformerSupplier<AuctionIdCntMax, AuctionIdCntMax> lcts;
    private final ArrayList<Long> topo1ProcLat;
    private final ArrayList<Long> topo2ProcLat;
    private final ArrayList<Long> bidsQueueTime;
    private final ArrayList<Long> auctionBidsQueueTime;
    private static final String TOPO1PROC_TAG = "subG1ProcLat";
    private static final String TOPO2PROC_TAG = "subG2ProcLat";
    private static final String BIDSQT_TAG = "bidsQueueDelay";
    private static final String AUCBIDSQT_TAG = "auctionBidsQueueDelay";

    public Query5(String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q5_sink_ets", baseDir,
            new IdentityValueMapper<>());
        topo1ProcLat = new ArrayList<>(NUM_STATS);
        topo2ProcLat = new ArrayList<>(NUM_STATS);
        bidsQueueTime = new ArrayList<>(NUM_STATS);
        auctionBidsQueueTime = new ArrayList<>(NUM_STATS);
    }

    @Override
    public StreamsBuilder getStreamBuilder(final String bootstrapServer, final String serde, final String configFile)
        throws IOException {
        final Properties prop = new Properties();
        final FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        final String outTp = prop.getProperty("out.name");
        final int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        final NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        final String bidsTp = prop.getProperty("bids.name");
        final String bidsTpRepar = prop.getProperty("bids.reparName");
        final int bidsTpPar = Integer.parseInt(prop.getProperty("bids.numPar"));
        final NewTopic bidsRepar = new NewTopic(bidsTp, bidsTpPar, REPLICATION_FACTOR);

        final String auctionBidsTp = prop.getProperty("auctionBids.name");
        final String auctionBidsTpRepar = prop.getProperty("auctionBids.reparName");
        final int auctionBidsTpPar = Integer.parseInt(prop.getProperty("auctionBids.numPar"));
        final NewTopic auctionBidsRepar = new NewTopic(auctionBidsTp, auctionBidsTpPar, REPLICATION_FACTOR);

        final List<NewTopic> nps = new ArrayList<>(3);
        nps.add(out);
        nps.add(bidsRepar);
        nps.add(auctionBidsRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        final Serde<Event> eSerde;
        final Serde<StartEndTime> seSerde;
        final Serde<AuctionIdCntMax> aicmSerde;
        final Serde<AuctionIdCount> aicSerde;
        final Serde<LongAndTime> latSerde;

        if (serde.equals("json")) {
            final JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            final JSONPOJOSerde<StartEndTime> seSerdeJSON = new JSONPOJOSerde<StartEndTime>();
            seSerdeJSON.setClass(StartEndTime.class);
            seSerde = seSerdeJSON;

            final JSONPOJOSerde<AuctionIdCount> aicSerdeJSON = new JSONPOJOSerde<AuctionIdCount>();
            aicSerdeJSON.setClass(AuctionIdCount.class);
            aicSerde = aicSerdeJSON;

            final JSONPOJOSerde<AuctionIdCntMax> aicmSerdeJSON = new JSONPOJOSerde<AuctionIdCntMax>();
            aicmSerdeJSON.setClass(AuctionIdCntMax.class);
            aicmSerde = aicmSerdeJSON;

            final JSONPOJOSerde<LongAndTime> latSerdeJSON = new JSONPOJOSerde<LongAndTime>();
            latSerdeJSON.setClass(LongAndTime.class);
            latSerde = latSerdeJSON;
        } else if (serde.equals("msgp")) {
            final MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            final MsgpPOJOSerde<StartEndTime> seSerdeMsgp = new MsgpPOJOSerde<>();
            seSerdeMsgp.setClass(StartEndTime.class);
            seSerde = seSerdeMsgp;

            final MsgpPOJOSerde<AuctionIdCount> aicSerdeMsgp = new MsgpPOJOSerde<>();
            aicSerdeMsgp.setClass(AuctionIdCount.class);
            aicSerde = aicSerdeMsgp;

            final MsgpPOJOSerde<AuctionIdCntMax> aicmSerdeMsgp = new MsgpPOJOSerde<>();
            aicmSerdeMsgp.setClass(AuctionIdCntMax.class);
            aicmSerde = aicmSerdeMsgp;

            final MsgpPOJOSerde<LongAndTime> latSerdeMsgp = new MsgpPOJOSerde<>();
            latSerdeMsgp.setClass(LongAndTime.class);
            latSerde = latSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
            .withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);
        final KStream<Long, Event> bid = inputs
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
                final long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(topo1ProcLat, procLat, TOPO1PROC_TAG);
                return value.bid.auction;
            });

        final TimeWindows tws = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(20))
            .advanceBy(Duration.ofSeconds(2));
        final WindowBytesStoreSupplier auctionBidsWSSupplier = Stores.inMemoryWindowStore("auctionBidsCountStore",
            Duration.ofMillis(tws.gracePeriodMs() + tws.size()), Duration.ofMillis(tws.size()), false);

        final KStream<StartEndTime, AuctionIdCount> auctionBids = bid
            .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                .withName(bidsTpRepar)
                .withNumberOfPartitions(bidsTpPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                assert value.injTsMs() != 0;
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(bidsQueueTime, queueDelay, BIDSQT_TAG);
                return value;
            })
            .groupByKey(Grouped.with(Serdes.Long(), eSerde))
            .windowedBy(tws)
            .aggregate(() -> new LongAndTime(0, 0, 0),
                (key, value, aggregate) -> {
                    // System.out.println("key: " + key + " ts: " + value.bid.dateTime + " agg: " +
                    // aggregate);
                    return new LongAndTime(aggregate.val + 1, 0, value.startProcTsNano());
                }, Named.as("auctionBidsCount"),
                Materialized.<Long, LongAndTime>as(auctionBidsWSSupplier)
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(latSerde)
                    .withCachingEnabled()
                    .withLoggingEnabled(new HashMap<>()))
            .toStream()
            .mapValues((key, value) -> {
                final AuctionIdCount aic = new AuctionIdCount(key.key(), value.val, Instant.now().toEpochMilli());
                assert value.startExecNano != 0;
                aic.startExecNano = value.startExecNano;
                return aic;
            })
            .selectKey((key, value) -> {
                StartEndTime se = new StartEndTime(key.window().start(), key.window().end(), 0);
                assert value.startExecNano != 0;
                final long procLat = System.nanoTime() - value.startExecNano;
                StreamsUtils.appendLat(topo2ProcLat, procLat, TOPO2PROC_TAG);
                return se;
            })
            .repartition(Repartitioned.with(seSerde, aicSerde)
                .withName(auctionBidsTpRepar)
                .withNumberOfPartitions(auctionBidsTpPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                assert value.injTsMs() != 0;
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(auctionBidsQueueTime, queueDelay, AUCBIDSQT_TAG);
                return value;
            });

        final KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");

        final KTable<StartEndTime, LongAndTime> maxBids = auctionBids
            .groupByKey(Grouped.with(seSerde, aicSerde))
            .aggregate(() -> new LongAndTime(0, 0, 0),
                (key, value, aggregate) -> {
                    // System.out.println("start " + key.startTime + " end: " + key.endTime +
                    // " aucId: " + value.aucId + " count: " + value.count +
                    // " aggregate: " + aggregate);
                    if (value.count > aggregate.val) {
                        return new LongAndTime(value.count, 0, value.startProcTsNano());
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
                final long startExecNano;
                if (leftValue.startProcTsNano() == 0) {
                    startExecNano = rightValue.startProcTsNano();
                } else if (rightValue.startProcTsNano() == 0) {
                    startExecNano = leftValue.startProcTsNano();
                } else {
                    startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
                }
                assert startExecNano != 0;
                AuctionIdCntMax aicm = new AuctionIdCntMax(leftValue.aucId,
                    leftValue.count, rightValue.val);
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
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q5");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
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
