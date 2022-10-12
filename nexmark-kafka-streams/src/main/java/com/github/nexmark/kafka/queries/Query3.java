package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.NameCityStateId;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Instant;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class Query3 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCountTransformerSupplier<NameCityStateId, NameCityStateId> lcts;
    public ArrayList<Long> aucProcLat;
    public ArrayList<Long> perProcLat;
    public ArrayList<Long> aucQueueTime;
    public ArrayList<Long> perQueueTime;

    public Query3(String baseDir) {
        input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q3_sink_ets",
                baseDir, new IdentityValueMapper<NameCityStateId>());
        aucProcLat = new ArrayList<Long>(NUM_STATS);
        perProcLat = new ArrayList<Long>(NUM_STATS);
        aucQueueTime = new ArrayList<Long>(NUM_STATS);
        perQueueTime = new ArrayList<Long>(NUM_STATS);
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

        String aucBySellerIDTab = prop.getProperty("aucBySellerIDTab.name");
        String aucBySellerIDTp = prop.getProperty("aucBySellerIDTp.name");
        int aucBySellerIDTpPar = Integer.parseInt(prop.getProperty("aucBySellerIDTp.numPar"));
        NewTopic auctionBySellerIdTabPar = new NewTopic(aucBySellerIDTp, aucBySellerIDTpPar, REPLICATION_FACTOR);

        String personsByIDTab = prop.getProperty("personsByIDTab.name");
        String personsByIDTp = prop.getProperty("personsByIDTp.name");
        int personsByIDTpPar = Integer.parseInt(prop.getProperty("personsByIDTp.numPar"));
        NewTopic persionsByIdTabPar = new NewTopic(personsByIDTp, personsByIDTpPar, REPLICATION_FACTOR);

        List<NewTopic> nps = new ArrayList<NewTopic>(3);
        nps.add(out);
        nps.add(auctionBySellerIdTabPar);
        nps.add(persionsByIdTabPar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Event> eSerde;
        Serde<NameCityStateId> ncsiSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<NameCityStateId> ncsiSerdeJSON = new JSONPOJOSerde<>();
            ncsiSerdeJSON.setClass(NameCityStateId.class);
            ncsiSerde = ncsiSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<NameCityStateId> ncsiSerdeMsgp = new MsgpPOJOSerde<>();
            ncsiSerdeMsgp.setClass(NameCityStateId.class);
            ncsiSerde = ncsiSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor())).peek(input);

        Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
                .branch((key, value) -> value != null && value.etype == Event.EType.AUCTION
                        && value.newAuction.category == 10,
                        Branched.as("aucBySeller"))
                .branch((key, value) -> value != null && value.etype == Event.EType.PERSON
                        && (value.newPerson.state.equals("OR") ||
                                value.newPerson.state.equals("ID") ||
                                value.newPerson.state.equals("CA")), Branched.as("personsById"))
                .noDefaultBranch();

        KeyValueBytesStoreSupplier auctionsBySellerIdKVStoreSupplier = Stores
                .inMemoryKeyValueStore("auctionBySellerIdKV");
        KTable<Long, Event> auctionsBySellerId = ksMap.get("Branch-aucBySeller")
                .selectKey((key, value) -> {
                    long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(aucProcLat, procLat, "subGAuc_proc");
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return value.newAuction.seller;
                })
                .toTable(Named.as(aucBySellerIDTab),
                        Materialized.<Long, Event>as(auctionsBySellerIdKVStoreSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde))
                .mapValues(new ValueMapper<Event, Event>() {
                    @Override
                    public Event apply(Event value) {
                        value.setStartProcTsNano(System.nanoTime());
                        long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                        StreamsUtils.appendLat(aucQueueTime, queueDelay, "aucQueueDelay");
                        return value;
                    }
                });

        KeyValueBytesStoreSupplier personsByIdKVStoreSupplier = Stores.inMemoryKeyValueStore("personsByIdKV");
        KTable<Long, Event> personsById = ksMap.get("Branch-personsById")
                .selectKey((key, value) -> {
                    long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(perProcLat, procLat, "subGPer_proc");
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return value.newPerson.id;
                })
                .toTable(Named.as(personsByIDTab),
                        Materialized.<Long, Event>as(personsByIdKVStoreSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde))
                .mapValues(new ValueMapper<Event, Event>() {
                    @Override
                    public Event apply(Event value) {
                        value.setStartProcTsNano(System.nanoTime());
                        long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                        StreamsUtils.appendLat(perQueueTime, queueDelay, "perQueueDelay");
                        return value;
                    }
                });
        auctionsBySellerId
                .join(personsById,
                        (leftValue, rightValue) -> {
                            NameCityStateId ret = new NameCityStateId(
                                    rightValue.newPerson.name,
                                    rightValue.newPerson.city,
                                    rightValue.newPerson.state,
                                    rightValue.newPerson.id);
                            ret.setStartProcTsNano(rightValue.startProcTsNano());
                            return ret;
                        })
                .toStream()
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), ncsiSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q3");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms, disableCache);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q3");
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

    public void waitForFinish() {
        lcts.waitForFinish();
    }

    @Override
    public void outputRemainingStats() {
        lcts.outputRemainingStats();
    }

    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
