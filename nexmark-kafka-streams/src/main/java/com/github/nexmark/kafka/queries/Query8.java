package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PersonTime;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.io.IOException;
import java.io.FileInputStream;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class Query8 implements NexmarkQuery {
    // public CountAction<Event> input;
    public LatencyCountTransformerSupplier<PersonTime, PersonTime> lcts;
    public ArrayList<Long> aucProcLat;
    public ArrayList<Long> perProcLat;
    public ArrayList<Long> aucQueueTime;
    public ArrayList<Long> perQueueTime;

    private static final String AUC_PROC_TAG = "subAuc_proc";
    private static final String PER_PROC_TAG = "subPer_proc";
    private static final String AUC_QUEUE_TAG = "auc_queue";
    private static final String PER_QUEUE_TAG = "per_queue";

    public Query8(String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q8_sink_ets", baseDir, new IdentityValueMapper<PersonTime>());
        aucProcLat = new ArrayList<Long>(NUM_STATS);
        perProcLat = new ArrayList<Long>(NUM_STATS);
        aucQueueTime = new ArrayList<Long>(NUM_STATS);
        perQueueTime = new ArrayList<Long>(NUM_STATS);
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        String aucBySellerIDTp = prop.getProperty("aucBySellerIDTp.name");
        String aucBySellerIDTpRepar = prop.getProperty("aucBySellerIDTp.reparName");
        int aucBySellerIDTpPar = Integer.parseInt(prop.getProperty("aucBySellerIDTp.numPar"));
        NewTopic auctionRepar = new NewTopic(aucBySellerIDTp, aucBySellerIDTpPar, REPLICATION_FACTOR);

        String personsByIDTp = prop.getProperty("personsByIDTp.name");
        String personsByIDTpRepar = prop.getProperty("personsByIDTp.reparName");
        int personsByIDTpPar = Integer.parseInt(prop.getProperty("personsByIDTp.numPar"));
        NewTopic personRepar = new NewTopic(personsByIDTp, personsByIDTpPar, REPLICATION_FACTOR);

        List<NewTopic> nps = new ArrayList<>(3);
        nps.add(out);
        nps.add(personRepar);
        nps.add(auctionRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Event> eSerde;
        Serde<PersonTime> ptSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<PersonTime> ptSerdeJSON = new JSONPOJOSerde<>();
            ptSerdeJSON.setClass(PersonTime.class);
            ptSerde = ptSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<PersonTime> ptSerdeMsgp = new MsgpPOJOSerde<>();
            ptSerdeMsgp.setClass(PersonTime.class);
            ptSerde = ptSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);
        Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
                .branch((key, value) -> {
                    if (value != null) {
                        value.setStartProcTsNano(System.nanoTime());
                        value.setInjTsMs(Instant.now().toEpochMilli());
                        return value.etype == Event.EType.PERSON;
                    } else {
                        return false;
                    }
                }, Branched.as("persons"))
                .branch((key, value) -> {
                    if (value != null) {
                        value.setStartProcTsNano(System.nanoTime());
                        value.setInjTsMs(Instant.now().toEpochMilli());
                        return value.etype == Event.EType.AUCTION;
                    } else {
                        return false;
                    }
                }, Branched.as("auctions"))
                .noDefaultBranch();

        KStream<Long, Event> person = ksMap.get("Branch-persons").selectKey((key, value) -> {
            long procLat = System.nanoTime() - value.startProcTsNano();
            StreamsUtils.appendLat(perProcLat, procLat, PER_PROC_TAG);
            return value.newPerson.id;
        }).repartition(Repartitioned.with(Serdes.Long(), eSerde)
                .withName(personsByIDTpRepar)
                .withNumberOfPartitions(personsByIDTpPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(perQueueTime, queueDelay, PER_QUEUE_TAG);
                    return value;
                });

        KStream<Long, Event> auction = ksMap.get("Branch-auctions")
                .selectKey((key, value) -> {
                    long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(aucProcLat, procLat, AUC_PROC_TAG);
                    return value.newAuction.seller;
                })
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(aucBySellerIDTpRepar)
                        .withNumberOfPartitions(aucBySellerIDTpPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(aucQueueTime, queueDelay, AUC_QUEUE_TAG);
                    return value;
                });

        long windowSizeMs = 10 * 1000;
        JoinWindows jw = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5));
        WindowBytesStoreSupplier auctionStoreSupplier = Stores.inMemoryWindowStore(
                "auction-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);
        WindowBytesStoreSupplier personStoreSupplier = Stores.inMemoryWindowStore(
                "person-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);
        StreamJoined<Long, Event, Event> sj = StreamJoined
                .<Long, Event, Event>with(auctionStoreSupplier, personStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(eSerde)
                .withLoggingEnabled(new HashMap<>());
        auction.join(person, (left, right) -> {
            long startExecNano = 0;
            if (left.startProcTsNano() == 0) {
                startExecNano = right.startProcTsNano();
            } else if (right.startProcTsNano() == 0) {
                startExecNano = left.startProcTsNano();
            } else {
                startExecNano = Math.min(left.startProcTsNano(), right.startProcTsNano());
            }
            assert startExecNano != 0;
            long ts = right.newPerson.dateTime;
            long windowStart = (Math.max(0, ts - windowSizeMs + windowSizeMs) / windowSizeMs) * windowSizeMs;
            PersonTime pt = new PersonTime(right.newPerson.id, right.newPerson.name, windowStart);
            pt.setStartProcTsNano(startExecNano);
            return pt;
        }, jw, sj)
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), ptSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache, boolean disableBatching) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
                disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q8");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(String bootstrapServer, int duration, int flushms,
            boolean disableCache, boolean disableBatching) {
        Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
                disableCache, disableBatching);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q8");
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
        StreamsUtils.printRemaining(perProcLat, PER_PROC_TAG);
        StreamsUtils.printRemaining(aucQueueTime, AUC_QUEUE_TAG);
        StreamsUtils.printRemaining(perQueueTime, PER_QUEUE_TAG);
    }

    // @Override
    // public void printRemainingStats() {
    // lcts.printRemainingStats();
    // }
}
