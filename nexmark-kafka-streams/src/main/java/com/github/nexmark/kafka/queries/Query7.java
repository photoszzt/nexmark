package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.LongAndTime;
import com.github.nexmark.kafka.model.StartEndTime;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.io.IOException;
import java.io.FileInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.HashMap;


import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class Query7 implements NexmarkQuery {
    // public CountAction<Event> input;
    private final LatencyCountTransformerSupplier<BidAndMax, BidAndMax> lcts;
    private final ArrayList<Long> bidsByWinProcLat;
    private final ArrayList<Long> bidsByPriceProcLat;
    private final ArrayList<Long> bidsByWinQueueTime;
    private final ArrayList<Long> bidsByPriceQueueTime;
    private final ArrayList<Long> maxBidsQueueTime;
    private final ArrayList<Long> topo2ProcLat;

    private static final String BIDS_BY_WIN_PROC_TAG = "bids_by_win_proc";
    private static final String BIDS_BY_PRICE_PROC_TAG = "bids_by_price_proc";
    private static final String BIDS_BY_WIN_QUEUE_TAG = "bids_by_win_queue";
    private static final String BIDS_BY_PRICE_QUEUE_TAG = "bids_by_price_queue";
    private static final String MAX_BIDS_QUEUE_TAG = "max_bids_queue";
    private static final String TOPO2_PROC_TAG = "topo2_proc";

    public Query7(final String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q7_sink_ets", baseDir, new IdentityValueMapper<BidAndMax>());
        bidsByWinProcLat = new ArrayList<>(NUM_STATS);
        bidsByPriceProcLat = new ArrayList<>(NUM_STATS);
        bidsByWinQueueTime = new ArrayList<>(NUM_STATS);
        bidsByPriceQueueTime = new ArrayList<>(NUM_STATS);
        topo2ProcLat = new ArrayList<>(NUM_STATS);
        maxBidsQueueTime = new ArrayList<>(NUM_STATS);
    }

    @Override
    public StreamsBuilder getStreamBuilder(final String bootstrapServer, final String serde, final String configFile) throws IOException {
        final Properties prop = new Properties();
        final FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        final String outTp = prop.getProperty("out.name");
        final int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        final NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        final String bidsByWinTp = prop.getProperty("bidsByWin.name");
        final String bidsByWinTpRepar = prop.getProperty("bidsByWin.reparName");
        final int bidsByWinTpPar = Integer.parseInt(prop.getProperty("bidsByWin.numPar"));
        final NewTopic bidsByWinRepar = new NewTopic(bidsByWinTp, bidsByWinTpPar, REPLICATION_FACTOR);

        final String bidsByPriceTp = prop.getProperty("bidsByPrice.name");
        final String bidsByPriceTpRepar = prop.getProperty("bidsByPrice.reparName");
        final int bidsByPriceTpPar = Integer.parseInt(prop.getProperty("bidsByPrice.numPar"));
        final NewTopic bidsByPriceRepar = new NewTopic(bidsByPriceTp, bidsByPriceTpPar, REPLICATION_FACTOR);

        final String maxBidsByPriceTp = prop.getProperty("maxBidsByPrice.name");
        final String maxBidsByPriceTpRepar = prop.getProperty("maxBidsByPrice.reparName");
        final int maxBidsByPriceTpPar = Integer.parseInt(prop.getProperty("maxBidsByPrice.numPar"));
        final NewTopic maxBidsByPriceRepar = new NewTopic(maxBidsByPriceTp, maxBidsByPriceTpPar, REPLICATION_FACTOR);

        final List<NewTopic> nps = new ArrayList<>(4);
        nps.add(out);
        nps.add(bidsByWinRepar);
        nps.add(bidsByPriceRepar);
        nps.add(maxBidsByPriceRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        final Serde<Event> eSerde;
        final Serde<BidAndMax> bmSerde;
        final Serde<StartEndTime> seSerde;
        final Serde<LongAndTime> latSerde;
        if (serde.equals("json")) {
            final JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            final JSONPOJOSerde<BidAndMax> bmSerdeJSON = new JSONPOJOSerde<>();
            bmSerdeJSON.setClass(BidAndMax.class);
            bmSerde = bmSerdeJSON;

            final JSONPOJOSerde<StartEndTime> seSerdeJSON = new JSONPOJOSerde<>();
            seSerdeJSON.setClass(StartEndTime.class);
            seSerde = seSerdeJSON;

            final JSONPOJOSerde<LongAndTime> latSerdeJSON = new JSONPOJOSerde<>();
            latSerdeJSON.setClass(LongAndTime.class);
            latSerde = latSerdeJSON;

        } else if (serde.equals("msgp")) {
            final MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            final MsgpPOJOSerde<BidAndMax> bmSerdeMsgp = new MsgpPOJOSerde<>();
            bmSerdeMsgp.setClass(BidAndMax.class);
            bmSerde = bmSerdeMsgp;

            final MsgpPOJOSerde<StartEndTime> seSerdeMsgp = new MsgpPOJOSerde<>();
            seSerdeMsgp.setClass(StartEndTime.class);
            seSerde = seSerdeMsgp;

            final MsgpPOJOSerde<LongAndTime> latSerdeMsgp = new MsgpPOJOSerde<>();
            latSerdeMsgp.setClass(LongAndTime.class);
            latSerde = latSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Event> inputs = builder.stream("nexmark_src",
            Consumed.with(Serdes.String(), eSerde).withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);

        final Duration windowSize = Duration.ofSeconds(10);
        final Duration grace = Duration.ofSeconds(5);

        final TimeWindows tw = TimeWindows.ofSizeAndGrace(windowSize, grace);

        final KStream<String, Event> bids = inputs.filter((key, value) -> {
            if (value != null) {
                value.setStartProcTsNano(System.nanoTime());
                value.setInjTsMs(Instant.now().toEpochMilli());
                return value.etype == Event.EType.BID;
            } else {
                return false;
            }
        });
        final KStream<StartEndTime, Event> bidsByWin = bids
            .selectKey((key, value) -> {
                final long sizeMs = tw.sizeMs;
                final long advanceMs = tw.advanceMs;
                final long windowStart = (Math.max(0, value.bid.dateTime - sizeMs + advanceMs) / advanceMs)
                    * advanceMs;
                final long wEnd = windowStart + sizeMs;
                final long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(bidsByWinProcLat, procLat, BIDS_BY_WIN_PROC_TAG);
                return new StartEndTime(windowStart, wEnd, 0);
            })
            .repartition(Repartitioned.with(seSerde, eSerde)
                .withName(bidsByWinTpRepar)
                .withNumberOfPartitions(bidsByWinTpPar));
        final KStream<Long, Event> bidsByPrice = bids
            .selectKey((key, value) -> {
                final long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(bidsByPriceProcLat, procLat, BIDS_BY_PRICE_PROC_TAG);
                return value.bid.price;
            })
            .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                .withName(bidsByPriceTpRepar)
                .withNumberOfPartitions(bidsByPriceTpPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(bidsByPriceQueueTime, queueDelay, BIDS_BY_PRICE_QUEUE_TAG);
                return value;
            });

        final String maxBidPerWindowTabName = "maxBidByWinTab";
        final KeyValueBytesStoreSupplier maxBidPerWindowTabSupplier = Stores.inMemoryKeyValueStore(maxBidPerWindowTabName);
        final KStream<StartEndTime, LongAndTime> maxBidPerWin = bidsByWin
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(bidsByWinQueueTime, queueDelay, BIDS_BY_WIN_QUEUE_TAG);
                return value;
            })
            .groupByKey(Grouped.with(seSerde, eSerde))
            .aggregate(() -> new LongAndTime(0, 0, 0), (key, value, aggregate) -> {
                if (value.bid.price > aggregate.val) {
                    return new LongAndTime(value.bid.price, 0, value.startProcTsNano());
                } else {
                    aggregate.setStartProcTsNano(value.startProcTsNano());
                    return aggregate;
                }
            }, Materialized.<StartEndTime, LongAndTime>as(maxBidPerWindowTabSupplier)
                .withCachingEnabled()
                .withLoggingEnabled(new HashMap<>())
                .withKeySerde(seSerde)
                .withValueSerde(latSerde))
            .toStream();
        final KStream<Long, StartEndTime> maxBidsByPrice = maxBidPerWin
            .map((key, value) -> {
                key.setInjTsMs(Instant.now().toEpochMilli());
                final long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(topo2ProcLat, procLat, TOPO2_PROC_TAG);
                return new KeyValue<>(value.val, key);
            })
            .repartition(Repartitioned.with(Serdes.Long(), seSerde)
                .withName(maxBidsByPriceTpRepar)
                .withNumberOfPartitions(maxBidsByPriceTpPar))
            .mapValues((key, value) -> {
                value.setStartProcTsNano(System.nanoTime());
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(maxBidsQueueTime, queueDelay, MAX_BIDS_QUEUE_TAG);
                return value;
            });

        final JoinWindows jw = JoinWindows.ofTimeDifferenceAndGrace(windowSize, grace);
        final Duration retension = Duration.ofMillis(jw.size() + jw.gracePeriodMs());
        final Duration winSize = Duration.ofMillis(jw.size());
        final WindowBytesStoreSupplier bByPStoreSupplier = Stores.inMemoryWindowStore(
            "bidsByPrice-join-store", retension, winSize, true);
        final WindowBytesStoreSupplier maxBidsByPStoreSupplier = Stores.inMemoryWindowStore(
            "maxBidsByPrice-join-store", retension, winSize, true);
        bidsByPrice.join(maxBidsByPrice, (leftValue, rightValue) -> {
                final long startExecNano;
                if (leftValue.startProcTsNano() == 0) {
                    startExecNano = rightValue.startProcTsNano();
                } else if (rightValue.startProcTsNano() == 0) {
                    startExecNano = leftValue.startProcTsNano();
                } else {
                    startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
                }
                assert startExecNano != 0;
                final BidAndMax bm = new BidAndMax(leftValue.bid.auction, leftValue.bid.price,
                    leftValue.bid.bidder, leftValue.bid.dateTime, rightValue.startTime, rightValue.endTime);
                bm.setStartProcTsNano(startExecNano);
                return bm;
            }, jw, StreamJoined.<Long, Event, StartEndTime>with(bByPStoreSupplier, maxBidsByPStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(seSerde)
                .withLoggingEnabled(new HashMap<>()))
            .filter((key, value) -> value.dateTimeMs >= value.wStartMs &&
                value.dateTimeMs <= value.wEndMs)
            .transformValues(lcts, Named.as("latency-measure"))
            .to(outTp, Produced.with(Serdes.Long(), bmSerde));

        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q7");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q7");
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
        StreamsUtils.printRemaining(bidsByWinProcLat, BIDS_BY_WIN_PROC_TAG);
        StreamsUtils.printRemaining(bidsByPriceProcLat, BIDS_BY_PRICE_PROC_TAG);
        StreamsUtils.printRemaining(bidsByWinQueueTime, BIDS_BY_WIN_QUEUE_TAG);
        StreamsUtils.printRemaining(bidsByPriceQueueTime, BIDS_BY_PRICE_QUEUE_TAG);
        StreamsUtils.printRemaining(maxBidsQueueTime, MAX_BIDS_QUEUE_TAG);
        StreamsUtils.printRemaining(topo2ProcLat, TOPO2_PROC_TAG);
    }

    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
