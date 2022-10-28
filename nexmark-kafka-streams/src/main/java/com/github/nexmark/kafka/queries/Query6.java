package com.github.nexmark.kafka.queries;

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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.github.nexmark.kafka.model.AucIDSeller;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.DoubleAndTime;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PriceTime;
import com.github.nexmark.kafka.model.PriceTimeList;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.admin.NewTopic;

import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;;

public class Query6 implements NexmarkQuery {
    // public CountAction<Event> input;
    private static final Duration auctionDurationUpperS = Duration.ofSeconds(1800);
    private final LatencyCountTransformerSupplier<DoubleAndTime, Double> lcts;
    private final ArrayList<Long> aucProcLat;
    private final ArrayList<Long> bidProcLat;
    private final ArrayList<Long> aucQueueTime;
    private final ArrayList<Long> bidQueueTime;
    private final ArrayList<Long> topo2ProcLat;
    private final ArrayList<Long> topo3ProcLat;
    private final ArrayList<Long> aucBidsQueueTime;
    private final ArrayList<Long> maxBidsQueueTime;

    private static final String AUC_PROC_TAG = "subGAuc_proc";
    private static final String BID_PROC_TAG = "subGBid_proc";
    private static final String AUCQT_TAG = "aucQueueTime";
    private static final String BIDQT_TAG = "bidQueueTime";
    private static final String TOPO2_PROC_TAG = "topo2_proc";
    private static final String TOPO3_PROC_TAG = "topo3_proc";
    private static final String AUCBIDSQT_TAG = "aucBidsQueueTime";
    private static final String MAXBIDSQT_TAG = "maxBidsQueueTime";

    public Query6(String baseDir) {
        // this.input = new CountAction<>();
        this.lcts = new LatencyCountTransformerSupplier<>("q6_sink_ets", baseDir,
            (value) -> value.avg);
        aucProcLat = new ArrayList<>(NUM_STATS);
        bidProcLat = new ArrayList<>(NUM_STATS);
        aucQueueTime = new ArrayList<>(NUM_STATS);
        bidQueueTime = new ArrayList<>(NUM_STATS);
        topo2ProcLat = new ArrayList<>(NUM_STATS);
        topo3ProcLat = new ArrayList<>(NUM_STATS);
        aucBidsQueueTime = new ArrayList<>(NUM_STATS);
        maxBidsQueueTime = new ArrayList<>(NUM_STATS);
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

        List<NewTopic> nps = new ArrayList<>(5);
        nps.add(out);
        nps.add(bidsByAucIDPar);
        nps.add(aucsByIDPar);
        nps.add(aucBidsPar);
        nps.add(maxBidsTpPar);
        StreamsUtils.createTopic(bootstrapServer, nps);
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Event> eSerde;
        Serde<AuctionBid> abSerde;
        Serde<AucIDSeller> asSerde;
        Serde<PriceTime> ptSerde;
        Serde<PriceTimeList> ptlSerde;

        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<AuctionBid> abSerdeJSON = new JSONPOJOSerde<AuctionBid>();
            abSerdeJSON.setClass(AuctionBid.class);
            abSerde = abSerdeJSON;

            JSONPOJOSerde<AucIDSeller> asSerdeJSON = new JSONPOJOSerde<AucIDSeller>();
            asSerdeJSON.setClass(AucIDSeller.class);
            asSerde = asSerdeJSON;

            JSONPOJOSerde<PriceTime> ptSerdeJSON = new JSONPOJOSerde<>();
            ptSerdeJSON.setClass(PriceTime.class);
            ptSerde = ptSerdeJSON;

            JSONPOJOSerde<PriceTimeList> ptlSerdeJSON = new JSONPOJOSerde<>();
            ptlSerdeJSON.setClass(PriceTimeList.class);
            ptlSerde = ptlSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<AuctionBid> abSerdeMsgp = new MsgpPOJOSerde<>();
            abSerdeMsgp.setClass(AuctionBid.class);
            abSerde = abSerdeMsgp;

            MsgpPOJOSerde<AucIDSeller> asSerdeMsgp = new MsgpPOJOSerde<>();
            asSerdeMsgp.setClass(AucIDSeller.class);
            asSerde = asSerdeMsgp;

            MsgpPOJOSerde<PriceTime> ptSerdeMsgp = new MsgpPOJOSerde<>();
            ptSerdeMsgp.setClass(PriceTime.class);
            ptSerde = ptSerdeMsgp;

            MsgpPOJOSerde<PriceTimeList> ptlSerdeMsgp = new MsgpPOJOSerde<>();
            ptlSerdeMsgp.setClass(PriceTimeList.class);
            ptlSerde = ptlSerdeMsgp;
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
                StreamsUtils.appendLat(bidProcLat, procLat, BID_PROC_TAG);
                return value.bid.auction;
            })
            .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                .withName(bidsByAucIDTpRepar)
                .withNumberOfPartitions(bidsByAucIDTpNumPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(bidQueueTime, queueDelay, BIDQT_TAG);
                return value;
            });
        KStream<Long, Event> aucsByID = ksMap.get("Branch-auctions")
            .selectKey((key, value) -> {
                long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(aucProcLat, procLat, AUC_PROC_TAG);
                return value.newAuction.id;
            })
            .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                .withName(aucsByIDTpRepar)
                .withNumberOfPartitions(aucsByIDTpNumPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(aucQueueTime, queueDelay, AUCQT_TAG);
                return value;
            });

        JoinWindows jw = JoinWindows.ofTimeDifferenceWithNoGrace(auctionDurationUpperS);
        WindowBytesStoreSupplier aucsByIDStoreSupplier = Stores.inMemoryWindowStore(
            "aucsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
            Duration.ofMillis(jw.size()), true);
        WindowBytesStoreSupplier bidsByAucIDStoreSupplier = Stores.inMemoryWindowStore(
            "bidsByID-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
            Duration.ofMillis(jw.size()), true);

        KStream<Long, AuctionBid> joined = aucsByID.join(bidsByAucID, (leftValue, rightValue) -> {
                long startExecNano = 0;
                if (leftValue.startProcTsNano() == 0) {
                    startExecNano = rightValue.startProcTsNano();
                } else if (rightValue.startProcTsNano() == 0) {
                    startExecNano = leftValue.startProcTsNano();
                } else {
                    startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
                }
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
            });

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");
        KTable<AucIDSeller, PriceTime> maxBids = joined
            .selectKey((key, value) -> {
                long lat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(topo2ProcLat, lat, TOPO2_PROC_TAG);
                return new AucIDSeller(key, value.seller);
            })
            .repartition(Repartitioned.with(asSerde, abSerde)
                .withName(aucBidsTpRepar)
                .withNumberOfPartitions(aucsByIDTpNumPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(aucBidsQueueTime, queueDelay, AUCBIDSQT_TAG);
                return value;
            })
            .groupByKey()
            .aggregate(() -> null,
                (key, value, aggregate) -> {
                    if (aggregate == null) {
                        return new PriceTime(value.bidPrice, value.bidDateTimeMs,
                            0, value.startProcTsNano());
                    }
                    if (value.bidPrice > aggregate.price) {
                        return new PriceTime(value.bidPrice, value.bidDateTimeMs,
                            0, value.startProcTsNano());
                    } else {
                        aggregate.setStartProcTsNano(value.startProcTsNano());
                        return aggregate;
                    }
                }, Named.as("maxBidPrice"), Materialized.<AucIDSeller, PriceTime>as(maxBidsKV)
                    .withCachingEnabled()
                    .withLoggingEnabled(new HashMap<>())
                    .withKeySerde(asSerde)
                    .withValueSerde(ptSerde));

        KeyValueBytesStoreSupplier collectValKV = Stores.inMemoryKeyValueStore("collectValKVStore");
        final int maxSize = 10;

        KTable<Long, PriceTimeList> aggTab = maxBids
            .groupBy((key, value) -> {
                assert value.startProcTsNano() != 0;
                long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(topo3ProcLat, procLat, TOPO3_PROC_TAG);
                value.injTsMs = Instant.now().toEpochMilli();
                return new KeyValue<>(key.seller, value);
            }, Grouped.with(Serdes.Long(), ptSerde).withName(maxBidsGroupByTab))
            .aggregate(() -> new PriceTimeList(new ArrayList<>(11), 0),
                (key, value, aggregate) -> {
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs;
                    StreamsUtils.appendLat(maxBidsQueueTime, queueDelay, MAXBIDSQT_TAG);
                    aggregate.ptlist.add(value);
                    aggregate.ptlist.sort(PriceTime.ASCENDING_TIME_THEN_PRICE);
                    // System.out.println("[ADD] agg before rm: " + aggregate);
                    if (aggregate.ptlist.size() > maxSize) {
                        aggregate.ptlist.remove(0);
                    }
                    // System.out.println("[ADD] agg after rm: " + aggregate);
                    aggregate.setStartProcTsNano(System.nanoTime());
                    return aggregate;
                },
                (key, value, aggregate) -> {
                    // System.out.println("[RM] val to rm: " + value);
                    if (aggregate.ptlist.size() > 0) {
                        aggregate.ptlist.remove(value);
                    }
                    // System.out.println("[RM] agg after rm: " + aggregate);
                    aggregate.setStartProcTsNano(System.nanoTime());
                    return aggregate;
                }, Named.as("collect-val"),
                Materialized.<Long, PriceTimeList>as(collectValKV)
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(ptlSerde)
                    .withLoggingEnabled(new HashMap<>())
                    .withCachingEnabled());
        aggTab.mapValues((key, value) -> {
                long sum = 0;
                int l = value.ptlist.size();
                for (PriceTime pt : value.ptlist) {
                    sum += pt.price;
                }
                double avg = (double) sum / (double) l;
                DoubleAndTime dt = new DoubleAndTime(avg);
                assert value.startProcTsNano() != 0;
                dt.setStartProcTsNano(value.startProcTsNano());
                return dt;
            }).toStream()
            .transformValues(lcts, Named.as("latency-measure"))
            .to(outTp, Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms,
                                               boolean disableCache, boolean disableBatching) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q6");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms,
                                               boolean disableCache, boolean disableBatching) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q6");
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
        StreamsUtils.printRemaining(aucProcLat, AUC_PROC_TAG);
        StreamsUtils.printRemaining(bidProcLat, BID_PROC_TAG);
        StreamsUtils.printRemaining(aucQueueTime, AUCQT_TAG);
        StreamsUtils.printRemaining(bidQueueTime, BIDQT_TAG);
        StreamsUtils.printRemaining(topo2ProcLat, TOPO2_PROC_TAG);
        StreamsUtils.printRemaining(topo3ProcLat, TOPO3_PROC_TAG);
        StreamsUtils.printRemaining(aucBidsQueueTime, AUCBIDSQT_TAG);
        StreamsUtils.printRemaining(maxBidsQueueTime, MAXBIDSQT_TAG);
    }
    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
