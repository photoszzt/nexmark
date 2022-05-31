package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AucIdCategory;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.SumAndCount;
import org.apache.kafka.clients.admin.NewTopic;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query4 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCountTransformerSupplier<Double> lcts;

    public Query4() {
        input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException {

        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        String bidsByAucIDTab = prop.getProperty("bidsByAucIDTab.name");
        String bidsByAucIDTp = prop.getProperty("bidsByAucIDTp.name");
        numPar = Integer.parseInt(prop.getProperty("bidsByAucIDTp.numPar"));
        NewTopic bidsByAucIDPar = new NewTopic(bidsByAucIDTp, numPar, REPLICATION_FACTOR);

        String aucsByIDTab = prop.getProperty("aucsByIDTab.name");
        String aucsByIDTp = prop.getProperty("aucsByIDTp.name");
        numPar = Integer.parseInt(prop.getProperty("aucsByIDTp.numPar"));
        NewTopic aucsByIDPar = new NewTopic(aucsByIDTp, numPar, REPLICATION_FACTOR);

        String aucBidsTp = prop.getProperty("aucBidsTp.name");
        String aucBidsTpRepar = prop.getProperty("aucBidsTp.reparName");
        int aucBidsTpNumPar = Integer.parseInt(prop.getProperty("aucBidsTp.numPar"));
        NewTopic aucBidsPar = new NewTopic(aucBidsTp, aucBidsTpNumPar, REPLICATION_FACTOR);

        String maxBidsTp = prop.getProperty("maxBidsTp.name");
        String maxBidsTpRepar = prop.getProperty("maxBidsTp.reparName");
        int maxBidsTpNumPar = Integer.parseInt(prop.getProperty("maxBidsTp.numPar"));
        NewTopic maxBidsTpPar = new NewTopic(maxBidsTp, maxBidsTpNumPar, REPLICATION_FACTOR);

        List<NewTopic> nps = new ArrayList<NewTopic>(3);
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

        KeyValueBytesStoreSupplier bidKVSupplier = Stores.inMemoryKeyValueStore("bidTab");
        KTable<Long, Event> bidsByAucID = inputs
                .filter((key, value) -> value.etype == Event.EType.BID)
                .selectKey((key, value) -> value.bid.auction)
                .toTable(Named.as(bidsByAucIDTab),
                        Materialized.<Long, Event>as(bidKVSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        KeyValueBytesStoreSupplier auctionKVSupplier = Stores.inMemoryKeyValueStore("auctionTab");
        KTable<Long, Event> aucsByID = inputs
                .filter((key, value) -> value.etype == Event.EType.AUCTION)
                .selectKey((key, value) -> value.newAuction.id)
                .toTable(Named.as(aucsByIDTab),
                        Materialized.<Long, Event>as(auctionKVSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        KeyValueBytesStoreSupplier maxBidsKV = Stores.inMemoryKeyValueStore("maxBidsKVStore");
        KTable<AucIdCategory, Long> maxBids = aucsByID
                .join(bidsByAucID, (leftValue, rightValue) -> new AuctionBid(rightValue.bid.dateTime,
                        leftValue.newAuction.dateTimeMs, leftValue.newAuction.expiresMs,
                        rightValue.bid.price, leftValue.newAuction.category))
                .filter((key, value) -> value.bidDateTimeMs >= value.aucDateTimeMs
                        && value.bidDateTimeMs <= value.aucExpiresMs)
                .toStream()
                .selectKey((key, value) -> new AucIdCategory(key, value.aucCategory))
                .repartition(Repartitioned.with(aicSerde, abSerde)
                        .withName(aucBidsTpRepar)
                        .withNumberOfPartitions(aucBidsTpNumPar))
                .groupByKey(Grouped.with(aicSerde, abSerde))
                .aggregate(() -> 0L, (key, value, aggregate) -> {
                    if (value.bidPrice > aggregate) {
                        return value.bidPrice;
                    } else {
                        return aggregate;
                    }
                }, Named.as("maxBidPrice"), Materialized.<AucIdCategory, Long>as(maxBidsKV)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(aicSerde)
                        .withValueSerde(Serdes.Long()));

        KeyValueBytesStoreSupplier sumCountKV = Stores.inMemoryKeyValueStore("sumCountKVStore");
        maxBids.toStream()
                .selectKey((key, value) -> key.category)
                .repartition(Repartitioned.with(Serdes.Long(), Serdes.Long()).withName(maxBidsTpRepar)
                        .withNumberOfPartitions(maxBidsTpNumPar))
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
                .aggregate(() -> new SumAndCount(0, 0),
                        (key, value, aggregate) -> new SumAndCount(aggregate.sum + value, aggregate.count + 1),
                        Named.as("sumCount"), Materialized.<Long, SumAndCount>as(sumCountKV)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(scSerde))
                .mapValues((key, value) -> (double) value.sum / (double) value.count)
                .toStream()
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer, int duration, int flushms) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer, duration, flushms);
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
    public List<Long> getRecordE2ELatency() {
        return lcts.GetLatency();
    }
}
