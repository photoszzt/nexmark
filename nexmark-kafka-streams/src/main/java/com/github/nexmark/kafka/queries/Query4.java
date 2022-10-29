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
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
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
    private final LatencyCountTransformerSupplier<DoubleAndTime, Double> lcts;
    private final ArrayList<Long> aucProcLat;
    private final ArrayList<Long> bidProcLat;
    private final ArrayList<Long> aucQueueTime;
    private final ArrayList<Long> bidQueueTime;
    private final ArrayList<Long> aucBidsQueueTime;
    private final ArrayList<Long> maxBidsQueueTime;
    private final ArrayList<Long> topo2ProcLat;
    private final ArrayList<Long> topo3ProcLat;
    private static final Duration AUCTION_DURATION_UPPER_S = Duration.ofSeconds(1800);

    public Query4(final String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q4_sink_ets", baseDir,
            value -> value.avg);
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
    public StreamsBuilder getStreamBuilder(final String bootstrapServer, final String serde,
                                           final String configFile) throws IOException {
        final Properties prop = new Properties();
        final FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        final String outTp = prop.getProperty("out.name");
        final int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        final NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        final String bidsByAucIDTp = prop.getProperty("bidsByAucIDTp.name");
        final String bidsByAucIDTpRepar = prop.getProperty("bidsByAucIDTp.reparName");
        final int bidsByAucIDTpNumPar = Integer.parseInt(prop.getProperty("bidsByAucIDTp.numPar"));
        final NewTopic bidsByAucIDPar = new NewTopic(bidsByAucIDTp, bidsByAucIDTpNumPar, REPLICATION_FACTOR);

        final String aucsByIDTp = prop.getProperty("aucsByIDTp.name");
        final int aucsByIDTpNumPar = Integer.parseInt(prop.getProperty("aucsByIDTp.numPar"));
        final String aucsByIDTpRepar = prop.getProperty("aucsByIDTp.reparName");
        final NewTopic aucsByIDPar = new NewTopic(aucsByIDTp, aucsByIDTpNumPar, REPLICATION_FACTOR);

        final String aucBidsTp = prop.getProperty("aucBidsTp.name");
        final String aucBidsTpRepar = prop.getProperty("aucBidsTp.reparName");
        final int aucBidsTpNumPar = Integer.parseInt(prop.getProperty("aucBidsTp.numPar"));
        final NewTopic aucBidsPar = new NewTopic(aucBidsTp, aucBidsTpNumPar, REPLICATION_FACTOR);

        final String maxBidsGroupByTab = prop.getProperty("maxBidsGroupByTab");
        final String maxBidsTp = prop.getProperty("maxBidsTp.name");
        final int maxBidsTpNumPar = Integer.parseInt(prop.getProperty("maxBidsTp.numPar"));
        final NewTopic maxBidsTpPar = new NewTopic(maxBidsTp, maxBidsTpNumPar, REPLICATION_FACTOR);

        final List<NewTopic> nps = new ArrayList<>(5);
        nps.add(out);
        nps.add(bidsByAucIDPar);
        nps.add(aucsByIDPar);
        nps.add(aucBidsPar);
        nps.add(maxBidsTpPar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<Event> eSerde;
        final Serde<AucIdCategory> aicSerde;
        final Serde<AuctionBid> abSerde;
        final Serde<SumAndCount> scSerde;
        final Serde<LongAndTime> ltSerde;
        if (serde.equals("json")) {
            final JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            final JSONPOJOSerde<AucIdCategory> aicSerdeJSON = new JSONPOJOSerde<>();
            aicSerdeJSON.setClass(AucIdCategory.class);
            aicSerde = aicSerdeJSON;

            final JSONPOJOSerde<AuctionBid> abSerdeJSON = new JSONPOJOSerde<>();
            abSerdeJSON.setClass(AuctionBid.class);
            abSerde = abSerdeJSON;

            final JSONPOJOSerde<SumAndCount> scSerdeJSON = new JSONPOJOSerde<>();
            scSerdeJSON.setClass(SumAndCount.class);
            scSerde = scSerdeJSON;

            final JSONPOJOSerde<LongAndTime> ltSerdeJSON = new JSONPOJOSerde<>();
            ltSerdeJSON.setClass(LongAndTime.class);
            ltSerde = ltSerdeJSON;

        } else if (serde.equals("msgp")) {
            final MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            final MsgpPOJOSerde<AucIdCategory> aicSerdeMsgp = new MsgpPOJOSerde<>();
            aicSerdeMsgp.setClass(AucIdCategory.class);
            aicSerde = aicSerdeMsgp;

            final MsgpPOJOSerde<AuctionBid> abSerdeMsgp = new MsgpPOJOSerde<>();
            abSerdeMsgp.setClass(AuctionBid.class);
            abSerde = abSerdeMsgp;

            final MsgpPOJOSerde<SumAndCount> scSerdeMsgp = new MsgpPOJOSerde<>();
            scSerdeMsgp.setClass(SumAndCount.class);
            scSerde = scSerdeMsgp;

            final MsgpPOJOSerde<LongAndTime> ltSerdeMsgp = new MsgpPOJOSerde<>();
            ltSerdeMsgp.setClass(LongAndTime.class);
            ltSerde = ltSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        final KStream<String, Event> inputs = builder.stream("nexmark_src",
            Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);
        final Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
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

        final KStream<Long, Event> bidsByAucID = ksMap.get("Branch-bids")
            .selectKey((key, value) -> {
                final long procLat = System.nanoTime() - value.startProcTsNano();
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
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(bidQueueTime, queueDelay, "bidQueueDelay");
                return value;
            }, Named.as("bidMapValues"));

        final KStream<Long, Event> aucsByID = ksMap.get("Branch-auctions")
            .selectKey((key, value) -> {
                final long procLat = System.nanoTime() - value.startProcTsNano();
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
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(aucQueueTime, queueDelay, "aucQueueDelay");
                return value;
            }, Named.as("aucMapValues"));

        final KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");

        final JoinWindows jw = JoinWindows.ofTimeDifferenceWithNoGrace(AUCTION_DURATION_UPPER_S);
        final WindowBytesStoreSupplier aucsByIDStoreSupplier = Stores.inMemoryWindowStore(
            "aucsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
            Duration.ofMillis(jw.size()), true);
        final WindowBytesStoreSupplier bidsByAucIDStoreSupplier = Stores.inMemoryWindowStore(
            "bidsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
            Duration.ofMillis(jw.size()), true);

        final KTable<AucIdCategory, LongAndTime> maxBids = aucsByID.join(bidsByAucID, (leftValue, rightValue) -> {
                final long startExecNano;
                if (leftValue.startProcTsNano() == 0) {
                    startExecNano = rightValue.startProcTsNano();
                } else if (rightValue.startProcTsNano() == 0) {
                    startExecNano = leftValue.startProcTsNano();
                } else {
                    startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
                }
                // System.out.println("leftStart: " + leftValue.startProcTsNano() + "
                // rightStart: " + rightValue.startProcTsNano() + " startExecNano: " +
                // startExecNano);
                assert startExecNano != 0;
                final AuctionBid ab = new AuctionBid(rightValue.bid.dateTime,
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
            .selectKey((key, value) -> {
                assert value.startProcTsNano() != 0;
                final long lat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(topo2ProcLat, lat, "subG2_proc");
                return new AucIdCategory(key, value.aucCategory);
            })
            .repartition(Repartitioned.with(aicSerde, abSerde)
                .withName(aucBidsTpRepar).withNumberOfPartitions(aucBidsTpNumPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(aucBidsQueueTime, queueDelay, "aucBidsQueueDelay");
                return value;
            }, Named.as("topo3-beg"))
            .groupByKey()
            .aggregate(() -> new LongAndTime(null, 0, 0),
                (key, value, aggregate) -> {
                    assert value.startProcTsNano() != 0;
                    if (aggregate.val == null) {
                        final LongAndTime lt = new LongAndTime(value.bidPrice, 0, value.startProcTsNano());
                        lt.startExecNano = value.startProcTsNano();
                        return lt;
                    }
                    if (value.bidPrice > aggregate.val) {
                        return new LongAndTime(value.bidPrice, 0, value.startProcTsNano());
                    } else {
                        aggregate.startExecNano = value.startProcTsNano();
                        return aggregate;
                    }
                }, Named.as("maxBidPrice"), Materialized.<AucIdCategory, LongAndTime>as(maxBidsKV)
                    .withCachingEnabled()
                    .withLoggingEnabled(new HashMap<>())
                    .withKeySerde(aicSerde)
                    .withValueSerde(ltSerde));

        final KeyValueBytesStoreSupplier sumCountKV = Stores.inMemoryKeyValueStore("sumCountKVStore");
        maxBids.groupBy((key, value) -> {
                // System.out.println("max, key: " + key + " value: " + value);
                final long procLat = System.nanoTime() - value.startExecNano;
                StreamsUtils.appendLat(topo3ProcLat, procLat, "subG3_proc");
                value.injTsMs = Instant.now().toEpochMilli();
                return new KeyValue<>(key.category, value);
            }, Grouped.with(Serdes.Long(), ltSerde).withName(maxBidsGroupByTab))
            .aggregate(() -> {
                    final SumAndCount s = new SumAndCount(0, 0, 0);
                    s.startExecNano = System.nanoTime();
                    return s;
                }, (key, value, aggregate) -> {
                    if (value != null) {
                        final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs;
                        StreamsUtils.appendLat(maxBidsQueueTime, queueDelay, "maxBidsQueueDelay");
                        final SumAndCount sc = new SumAndCount(aggregate.sum + value.val, aggregate.count + 1, 0);
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
                }, (key, value, aggregate) -> {
                    if (value != null) {
                        final SumAndCount sc = new SumAndCount(aggregate.sum - value.val, aggregate.count - 1, 0);
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
                }, Named.as("sumCount"),
                Materialized.<Long, SumAndCount>as(sumCountKV)
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(scSerde)
                    .withCachingEnabled()
                    .withLoggingEnabled(new HashMap<>()))
            .mapValues((key, value) -> {
                final DoubleAndTime d = new DoubleAndTime((double) value.sum / (double) value.count);
                assert value.startExecNano != 0;
                d.startExecNano = value.startExecNano;
                return d;
            })
            .toStream()
            .transformValues(lcts, Named.as("latency-measure"))
            .to(outTp, Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q4");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
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
