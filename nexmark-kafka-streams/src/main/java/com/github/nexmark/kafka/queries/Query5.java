package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AuctionIdCntMax;
import com.github.nexmark.kafka.model.AuctionIdCount;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.StartEndTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import java.util.HashMap;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query5 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCountTransformerSupplier<AuctionIdCntMax> lcts;

    public Query5() {
        input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>();
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
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor())).peek(input);
        KStream<Long, Event> bid = inputs
                .filter((key, value) -> value != null && value.etype == Event.EType.BID)
                .selectKey((key, value) -> value.bid.auction);

        TimeWindows tws = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(20))
                .advanceBy(Duration.ofSeconds(2));
        WindowBytesStoreSupplier auctionBidsWSSupplier = Stores.inMemoryWindowStore("auctionBidsCountStore",
                Duration.ofMillis(tws.gracePeriodMs() + tws.size()), Duration.ofMillis(tws.size()), false);

        KStream<StartEndTime, AuctionIdCount> auctionBids = bid
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(bidsTpRepar)
                        .withNumberOfPartitions(bidsTpPar))
                .groupByKey(Grouped.with(Serdes.Long(), eSerde))
                .windowedBy(tws)
                .aggregate(new Initializer<Long>() {
                    @Override
                    public Long apply() {
                        return 0L;
                    }
                }, new Aggregator<Long, Event, Long>() {
                    @Override
                    public Long apply(Long key, Event value, Long aggregate) {
                        // TODO Auto-generated method stub
                        // System.out.println("key: " + key + " ts: " + value.bid.dateTime + " agg: " + aggregate);
                        return aggregate + 1;
                    }
                }, Named.as("auctionBidsCount"),
                        Materialized.<Long, Long>as(auctionBidsWSSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(Serdes.Long())
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>()))
                .toStream()
                .mapValues((key, value) -> new AuctionIdCount(key.key(), value))
                .selectKey((key, value) -> new StartEndTime(key.window().start(), key.window().end()))
                .repartition(Repartitioned.with(seSerde, aicSerde)
                        .withName(auctionBidsTpRepar)
                        .withNumberOfPartitions(auctionBidsTpPar));

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");

        KTable<StartEndTime, Long> maxBids = auctionBids
                .groupByKey(Grouped.with(seSerde, aicSerde))
                .aggregate(() -> 0L,
                        (key, value, aggregate) -> {
                            // System.out.println("start " + key.startTime + " end: " + key.endTime +
                            //         " aucId: " + value.aucId + " count: " + value.count +
                            //         " aggregate: " + aggregate);
                            if (value.count > aggregate) {
                                return value.count;
                            } else {
                                return aggregate;
                            }
                        }, Named.as("maxBidsAgg"),
                        Materialized.<StartEndTime, Long>as(maxBidsKV)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(seSerde)
                                .withValueSerde(Serdes.Long()));
        auctionBids
                .join(maxBids, (leftValue, rightValue) -> new AuctionIdCntMax(leftValue.aucId,
                        leftValue.count, (long) rightValue))
                .filter((key, value) -> value.count >= value.maxCnt)
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(seSerde, aicmSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer, int duration, int flushms) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer, duration, flushms);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q5");
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
