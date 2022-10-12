package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.StartEndTime;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.io.IOException;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.*;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query7 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCountTransformerSupplier<BidAndMax, BidAndMax> lcts;

    public Query7(String baseDir) {
        input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q7_sink_ets", baseDir, new IdentityValueMapper<BidAndMax>());
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        String bidsByWinTp = prop.getProperty("bidsByWin.name");
        String bidsByWinTpRepar = prop.getProperty("bidsByWin.reparName");
        int bidsByWinTpPar = Integer.parseInt(prop.getProperty("bidsByWin.numPar"));
        NewTopic bidsByWinRepar = new NewTopic(bidsByWinTp, bidsByWinTpPar, REPLICATION_FACTOR);

        String bidsByPriceTp = prop.getProperty("bidsByPrice.name");
        String bidsByPriceTpRepar = prop.getProperty("bidsByPrice.reparName");
        int bidsByPriceTpPar = Integer.parseInt(prop.getProperty("bidsByPrice.numPar"));
        NewTopic bidsByPriceRepar = new NewTopic(bidsByPriceTp, bidsByPriceTpPar, REPLICATION_FACTOR);

        String maxBidsByPriceTp = prop.getProperty("maxBidsByPrice.name");
        String maxBidsByPriceTpRepar = prop.getProperty("maxBidsByPrice.reparName");
        int maxBidsByPriceTpPar = Integer.parseInt(prop.getProperty("maxBidsByPrice.numPar"));
        NewTopic maxBidsByPriceRepar = new NewTopic(maxBidsByPriceTp, maxBidsByPriceTpPar, REPLICATION_FACTOR);

        List<NewTopic> nps = new ArrayList<>(4);
        nps.add(out);
        nps.add(bidsByWinRepar);
        nps.add(bidsByPriceRepar);
        nps.add(maxBidsByPriceRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        Serde<Event> eSerde;
        Serde<BidAndMax> bmSerde;
        Serde<StartEndTime> seSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<BidAndMax> bmSerdeJSON = new JSONPOJOSerde<>();
            bmSerdeJSON.setClass(BidAndMax.class);
            bmSerde = bmSerdeJSON;

            JSONPOJOSerde<StartEndTime> seSerdeJSON = new JSONPOJOSerde<>();
            seSerdeJSON.setClass(StartEndTime.class);
            seSerde = seSerdeJSON;

        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<BidAndMax> bmSerdeMsgp = new MsgpPOJOSerde<>();
            bmSerdeMsgp.setClass(BidAndMax.class);
            bmSerde = bmSerdeMsgp;

            MsgpPOJOSerde<StartEndTime> seSerdeMsgp = new MsgpPOJOSerde<>();
            seSerdeMsgp.setClass(StartEndTime.class);
            seSerde = seSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde).withTimestampExtractor(new EventTimestampExtractor()))
                .peek(input);

        Duration windowSize = Duration.ofSeconds(10);
        Duration grace = Duration.ofSeconds(5);

        TimeWindows tw = TimeWindows.ofSizeAndGrace(windowSize, grace);

        KStream<String, Event> bids = inputs.filter((key, value) -> value != null && value.etype == Event.EType.BID);
        KStream<StartEndTime, Event> bidsByWin = bids
                .selectKey(new KeyValueMapper<String, Event, StartEndTime>() {
                    @Override
                    public StartEndTime apply(String key, Event value) {
                        long sizeMs = tw.sizeMs;
                        long advanceMs = tw.advanceMs;
                        long windowStart = (Math.max(0, value.bid.dateTime - sizeMs + advanceMs) / advanceMs)
                                * advanceMs;
                        long wEnd = windowStart + sizeMs;
                        return new StartEndTime(windowStart, wEnd);
                    }

                })
                .repartition(Repartitioned.<StartEndTime, Event>with(seSerde, eSerde)
                        .withName(bidsByWinTpRepar)
                        .withNumberOfPartitions(bidsByWinTpPar));
        KStream<Long, Event> bidsByPrice = bids
                .selectKey((key, value) -> value.bid.price)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(bidsByPriceTpRepar)
                        .withNumberOfPartitions(bidsByPriceTpPar));

        String maxBidPerWindowTabName = "maxBidByWinTab";
        KeyValueBytesStoreSupplier maxBidPerWindowTabSupplier = Stores.inMemoryKeyValueStore(maxBidPerWindowTabName);
        KStream<StartEndTime, Long> maxBidPerWin = bidsByWin
                .groupByKey(Grouped.with(seSerde, eSerde))
                .aggregate(() -> 0L, (key, value, aggregate) -> {
                    if (value.bid.price > aggregate) {
                        return value.bid.price;
                    } else {
                        return aggregate;
                    }
                }, Materialized.<StartEndTime, Long>as(maxBidPerWindowTabSupplier)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(seSerde)
                        .withValueSerde(Serdes.Long()))
                .toStream();
        KStream<Long, StartEndTime> maxBidsByPrice = maxBidPerWin
                .map(new KeyValueMapper<StartEndTime, Long, KeyValue<Long, StartEndTime>>() {
                    @Override
                    public KeyValue<Long, StartEndTime> apply(StartEndTime key, Long value) {
                        return new KeyValue<Long, StartEndTime>(value, key);
                    }
                })
                .repartition(Repartitioned.with(Serdes.Long(), seSerde)
                        .withName(maxBidsByPriceTpRepar)
                        .withNumberOfPartitions(maxBidsByPriceTpPar));

        JoinWindows jw = JoinWindows.ofTimeDifferenceAndGrace(windowSize, grace);
        Duration retension = Duration.ofMillis(jw.size() + jw.gracePeriodMs());
        Duration winSize = Duration.ofMillis(jw.size());
        WindowBytesStoreSupplier bByPStoreSupplier = Stores.inMemoryWindowStore(
                "bidsByPrice-join-store", retension, winSize, true);
        WindowBytesStoreSupplier maxBidsByPStoreSupplier = Stores.inMemoryWindowStore(
                "maxBidsByPrice-join-store", retension, winSize, true);
        bidsByPrice.join(maxBidsByPrice, new ValueJoiner<Event, StartEndTime, BidAndMax>() {
            @Override
            public BidAndMax apply(Event value1, StartEndTime value2) {
                return new BidAndMax(value1.bid.auction, value1.bid.price,
                        value1.bid.bidder, value1.bid.dateTime, value2.startTime, value2.endTime);
            }
        }, jw, StreamJoined.<Long, Event, StartEndTime>with(bByPStoreSupplier, maxBidsByPStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(seSerde)
                .withLoggingEnabled(new HashMap<>()))
                .filter(new Predicate<Long, BidAndMax>() {
                    @Override
                    public boolean test(Long key, BidAndMax value) {
                        return value.dateTimeMs >= value.wStartMs && value.dateTimeMs <= value.wEndMs;
                    }
                })
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), bmSerde));

        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q7");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q7");
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

    // @Override
    // public void waitForFinish() {
    // lcts.waitForFinish();
    // }

    @Override
    public void outputRemainingStats() {
        lcts.outputRemainingStats();
    }

    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
