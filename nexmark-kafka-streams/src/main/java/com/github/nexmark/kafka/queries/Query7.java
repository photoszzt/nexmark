package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.*;

import java.io.IOException;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.*;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query7 implements NexmarkQuery {
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

        List<NewTopic> nps = new ArrayList<>(2);
        nps.add(out);
        nps.add(bidsByWinRepar);
        nps.add(bidsByPriceRepar);
        nps.add(maxBidsByPriceRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        Serde<Event> eSerde;
        Serde<BidAndMax> bmSerde;
        Serde<TimeWindow> twSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<BidAndMax> bmSerdeJSON = new JSONPOJOSerde<>();
            bmSerdeJSON.setClass(BidAndMax.class);
            bmSerde = bmSerdeJSON;

            JSONPOJOSerde<TimeWindow> twSerdeJSON = new JSONPOJOSerde<>();
            twSerdeJSON.setClass(TimeWindow.class);
            twSerde = twSerdeJSON;

        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<BidAndMax> bmSerdeMsgp = new MsgpPOJOSerde<>();
            bmSerdeMsgp.setClass(BidAndMax.class);
            bmSerde = bmSerdeMsgp;

            MsgpPOJOSerde<TimeWindow> twSerdeMsgp = new MsgpPOJOSerde<>();
            twSerdeMsgp.setClass(TimeWindow.class);
            twSerde = twSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde).withTimestampExtractor(new EventTimestampExtractor()));

        Duration windowSize = Duration.ofSeconds(10);
        Duration grace = Duration.ofSeconds(5);

        TimeWindows tw = TimeWindows.ofSizeAndGrace(windowSize, grace);

        KStream<String, Event> bids = inputs.filter((key, value) -> value != null && value.etype == Event.EType.BID);
        KStream<TimeWindow, Event> bidsByWin = bids
                .selectKey(new KeyValueMapper<String, Event, TimeWindow>() {
                    @Override
                    public TimeWindow apply(String key, Event value) {
                        long sizeMs = tw.sizeMs;
                        long advanceMs = tw.advanceMs;
                        long windowStart = (Math.max(0, value.bid.dateTime - sizeMs + advanceMs) / advanceMs)
                                * advanceMs;
                        long wEnd = windowStart + sizeMs;
                        return new TimeWindow(windowStart, wEnd);
                    }

                })
                .repartition(Repartitioned.<TimeWindow, Event>with(twSerde, eSerde)
                        .withName(bidsByWinTpRepar)
                        .withNumberOfPartitions(bidsByWinTpPar));
        KStream<Long, Event> bidsByPrice = bids
                .selectKey((key, value) -> value.bid.price)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(bidsByPriceTpRepar)
                        .withNumberOfPartitions(bidsByPriceTpPar));

        String maxBidPerWindowTabName = "maxBidByWinTab";
        KeyValueBytesStoreSupplier maxBidPerWindowTabSupplier = Stores.inMemoryKeyValueStore(maxBidPerWindowTabName);
        KStream<TimeWindow, Long> maxBidPerWin = bidsByWin
                .groupByKey(Grouped.with(twSerde, eSerde))
                .aggregate(() -> 0L, (key, value, aggregate) -> {
                    if (value.bid.price > aggregate) {
                        return value.bid.price;
                    } else {
                        return aggregate;
                    }
                }, Materialized.<TimeWindow, Long>as(maxBidPerWindowTabSupplier)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(twSerde)
                        .withValueSerde(Serdes.Long()))
                .toStream();
        KStream<Long, TimeWindow> maxBidsByPrice = maxBidPerWin
                .map(new KeyValueMapper<TimeWindow, Long, KeyValue<Long, TimeWindow>>() {
                    @Override
                    public KeyValue<Long, TimeWindow> apply(TimeWindow key, Long value) {
                        return new KeyValue<Long, TimeWindow>(value, key);
                    }
                })
                .repartition(Repartitioned.with(Serdes.Long(), twSerde)
                        .withName(maxBidsByPriceTpRepar)
                        .withNumberOfPartitions(maxBidsByPriceTpPar));

        JoinWindows jw = JoinWindows.ofTimeDifferenceAndGrace(windowSize, grace);
        Duration retension = Duration.ofMillis(jw.size() + jw.gracePeriodMs());
        Duration winSize = Duration.ofMillis(jw.size());
        WindowBytesStoreSupplier bByPStoreSupplier = Stores.inMemoryWindowStore(
                "bidsByPrice-join-store", retension, winSize, true);
        WindowBytesStoreSupplier maxBidsByPStoreSupplier = Stores.inMemoryWindowStore(
                "maxBidsByPrice-join-store", retension, winSize, true);
        bidsByPrice.join(maxBidsByPrice, new ValueJoiner<Event, TimeWindow, BidAndMax>() {
            @Override
            public BidAndMax apply(Event value1, TimeWindow value2) {
                return new BidAndMax(value1.bid.auction, value1.bid.price,
                        value1.bid.bidder, value1.bid.dateTime, value2.start(), value2.end());
            }
        }, jw, StreamJoined.<Long, Event, TimeWindow>with(bByPStoreSupplier, maxBidsByPStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(twSerde)
                .withLoggingEnabled(new HashMap<>())).to(outTp, Produced.with(Serdes.Long(), bmSerde));

        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer, int duration, int flushms) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer, duration, flushms);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q7");
        return props;
    }

    @Override
    public long getInputCount() {
        return 0;
    }

    @Override
    public void setAfterWarmup() {
    }

    @Override
    public List<Long> getRecordE2ELatency() {
        return null;
    }
}
