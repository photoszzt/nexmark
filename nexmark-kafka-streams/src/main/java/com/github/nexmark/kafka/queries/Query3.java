package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.NameCityStateId;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
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
    private final LatencyCountTransformerSupplier<NameCityStateId, NameCityStateId> lcts;
    private final ArrayList<Long> aucProcLat;
    private final ArrayList<Long> perProcLat;
    private final ArrayList<Long> aucQueueTime;
    private final ArrayList<Long> perQueueTime;

    public Query3(final String baseDir) {
        lcts = new LatencyCountTransformerSupplier<>("q3_sink_ets",
            baseDir, new IdentityValueMapper<>());
        aucProcLat = new ArrayList<>(NUM_STATS);
        perProcLat = new ArrayList<>(NUM_STATS);
        aucQueueTime = new ArrayList<>(NUM_STATS);
        perQueueTime = new ArrayList<>(NUM_STATS);
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

        final String aucBySellerIDTab = prop.getProperty("aucBySellerIDTab.name");
        final String aucBySellerIDTp = prop.getProperty("aucBySellerIDTp.name");
        final int aucBySellerIDTpPar = Integer.parseInt(prop.getProperty("aucBySellerIDTp.numPar"));
        final NewTopic auctionBySellerIdTabPar = new NewTopic(aucBySellerIDTp, aucBySellerIDTpPar, REPLICATION_FACTOR);

        final String personsByIDTab = prop.getProperty("personsByIDTab.name");
        final String personsByIDTp = prop.getProperty("personsByIDTp.name");
        final int personsByIDTpPar = Integer.parseInt(prop.getProperty("personsByIDTp.numPar"));
        final NewTopic persionsByIdTabPar = new NewTopic(personsByIDTp, personsByIDTpPar, REPLICATION_FACTOR);

        final List<NewTopic> nps = new ArrayList<>(3);
        nps.add(out);
        nps.add(auctionBySellerIdTabPar);
        nps.add(persionsByIdTabPar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<Event> eSerde;
        final Serde<NameCityStateId> ncsiSerde;
        if (serde.equals("json")) {
            final JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            final JSONPOJOSerde<NameCityStateId> ncsiSerdeJSON = new JSONPOJOSerde<>();
            ncsiSerdeJSON.setClass(NameCityStateId.class);
            ncsiSerde = ncsiSerdeJSON;
        } else if (serde.equals("msgp")) {
            final MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            final MsgpPOJOSerde<NameCityStateId> ncsiSerdeMsgp = new MsgpPOJOSerde<>();
            ncsiSerdeMsgp.setClass(NameCityStateId.class);
            ncsiSerde = ncsiSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        final KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
            .withTimestampExtractor(new EventTimestampExtractor()));

        final Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
            .branch((key, value) -> {
                if (value != null) {
                    value.setStartProcTsNano(System.nanoTime());
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return value.etype == Event.EType.AUCTION
                        && value.newAuction.category == 10;
                } else {
                    return false;
                }
            }, Branched.as("aucBySeller"))
            .branch((key, value) -> {
                if (value != null) {
                    value.setStartProcTsNano(System.nanoTime());
                    value.setInjTsMs(Instant.now().toEpochMilli());
                    return value.etype == Event.EType.PERSON && value.newPerson != null
                        && (value.newPerson.state.equals("OR") ||
                        value.newPerson.state.equals("ID") ||
                        value.newPerson.state.equals("CA"));
                } else {
                    return false;
                }
            }, Branched.as("personsById"))
            .noDefaultBranch();

        final KeyValueBytesStoreSupplier auctionsBySellerIdKVStoreSupplier = Stores
            .inMemoryKeyValueStore("auctionBySellerIdKV");
        final KTable<Long, Event> auctionsBySellerId = ksMap.get("Branch-aucBySeller")
            .selectKey((key, value) -> {
                final long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(aucProcLat, procLat, "subGAuc_proc");
                return value.newAuction.seller;
            })
            .toTable(Named.as(aucBySellerIDTab),
                Materialized.<Long, Event>as(auctionsBySellerIdKVStoreSupplier)
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(eSerde))
            .mapValues(value -> {
                value.setStartProcTsNano(System.nanoTime());
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(aucQueueTime, queueDelay, "aucQueueDelay");
                return value;
            });

        final KeyValueBytesStoreSupplier personsByIdKVStoreSupplier = Stores.inMemoryKeyValueStore("personsByIdKV");
        final KTable<Long, Event> personsById = ksMap.get("Branch-personsById")
            .selectKey((key, value) -> {
                final long procLat = System.nanoTime() - value.startProcTsNano();
                StreamsUtils.appendLat(perProcLat, procLat, "subGPer_proc");
                return value.newPerson.id;
            })
            .toTable(Named.as(personsByIDTab),
                Materialized.<Long, Event>as(personsByIdKVStoreSupplier)
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(eSerde))
            .mapValues(value -> {
                value.setStartProcTsNano(System.nanoTime());
                final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                StreamsUtils.appendLat(perQueueTime, queueDelay, "perQueueDelay");
                return value;
            });
        auctionsBySellerId
            .join(personsById,
                (leftValue, rightValue) -> {
                    final long startExecNano;
                    if (leftValue.startProcTsNano() == 0) {
                        startExecNano = rightValue.startProcTsNano();
                    } else if (rightValue.startProcTsNano() == 0) {
                        startExecNano = leftValue.startProcTsNano();
                    } else {
                        startExecNano = Math.min(leftValue.startProcTsNano(), rightValue.startProcTsNano());
                    }
                    assert startExecNano != 0;
                    final NameCityStateId ret = new NameCityStateId(
                        rightValue.newPerson.name,
                        rightValue.newPerson.city,
                        rightValue.newPerson.state,
                        rightValue.newPerson.id);
                    ret.setStartProcTsNano(startExecNano);
                    return ret;
                })
            .toStream()
            .transformValues(lcts, Named.as("latency-measure"))
            .to(outTp, Produced.with(Serdes.Long(), ncsiSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching, producerBatchSize);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q3");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching, producerBatchSize);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q3");
        return props;
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
        StreamsUtils.printRemaining(aucProcLat, "subGAuc_proc");
        StreamsUtils.printRemaining(perProcLat, "subGPer_proc");
        StreamsUtils.printRemaining(aucQueueTime, "aucQueueDelay");
        StreamsUtils.printRemaining(perQueueTime, "perQueueDelay");
    }

    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
