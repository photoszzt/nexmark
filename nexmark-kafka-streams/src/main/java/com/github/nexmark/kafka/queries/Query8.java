package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PersonTime;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.io.IOException;
import java.io.FileInputStream;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class Query8 implements NexmarkQuery {
    // public CountAction<Event> input;
    private final LatencyCountTransformerSupplier<PersonTime, PersonTime> lcts;
    private final ArrayList<Long> aucProcLat;
    private final ArrayList<Long> perProcLat;
    private final ArrayList<Long> aucQueueTime;
    private final ArrayList<Long> perQueueTime;

    private static final String AUC_PROC_TAG = "subAuc_proc";
    private static final String PER_PROC_TAG = "subPer_proc";
    private static final String AUC_QUEUE_TAG = "auc_queue";
    private static final String PER_QUEUE_TAG = "per_queue";

    public Query8(final String baseDir) {
        // input = new CountAction<>();
        lcts = new LatencyCountTransformerSupplier<>("q8_sink_ets", baseDir, new IdentityValueMapper<PersonTime>());
        aucProcLat = new ArrayList<>(NUM_STATS);
        perProcLat = new ArrayList<>(NUM_STATS);
        aucQueueTime = new ArrayList<>(NUM_STATS);
        perQueueTime = new ArrayList<>(NUM_STATS);
    }

    @Override
    public StreamsBuilder getStreamBuilder(final String bootstrapServer, final String serde, final String configFile) throws IOException {
        final Properties prop = new Properties();
        final FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        final String outTp = prop.getProperty("out.name");
        final int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        final NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        final String aucBySellerIDTp = prop.getProperty("aucBySellerIDTp.name");
        final String aucBySellerIDTpRepar = prop.getProperty("aucBySellerIDTp.reparName");
        final int aucBySellerIDTpPar = Integer.parseInt(prop.getProperty("aucBySellerIDTp.numPar"));
        final NewTopic auctionRepar = new NewTopic(aucBySellerIDTp, aucBySellerIDTpPar, REPLICATION_FACTOR);

        final String personsByIDTp = prop.getProperty("personsByIDTp.name");
        final String personsByIDTpRepar = prop.getProperty("personsByIDTp.reparName");
        final int personsByIDTpPar = Integer.parseInt(prop.getProperty("personsByIDTp.numPar"));
        final NewTopic personRepar = new NewTopic(personsByIDTp, personsByIDTpPar, REPLICATION_FACTOR);

        final List<NewTopic> nps = new ArrayList<>(3);
        nps.add(out);
        nps.add(personRepar);
        nps.add(auctionRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<Event> eSerde;
        final Serde<PersonTime> ptSerde;
        if (serde.equals("json")) {
            final JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            final JSONPOJOSerde<PersonTime> ptSerdeJSON = new JSONPOJOSerde<>();
            ptSerdeJSON.setClass(PersonTime.class);
            ptSerde = ptSerdeJSON;
        } else if (serde.equals("msgp")) {
            final MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            final MsgpPOJOSerde<PersonTime> ptSerdeMsgp = new MsgpPOJOSerde<>();
            ptSerdeMsgp.setClass(PersonTime.class);
            ptSerde = ptSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        final KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));
        // .peek(input);
        final Map<String, KStream<String, Event>> ksMap = inputs.split(Named.as("Branch-"))
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

        final KStream<Long, Event> person = ksMap.get("Branch-persons").selectKey((key, value) -> {
            final long procLat = System.nanoTime() - value.startProcTsNano();
            StreamsUtils.appendLat(perProcLat, procLat, PER_PROC_TAG);
            return value.newPerson.id;
        }).repartition(Repartitioned.with(Serdes.Long(), eSerde)
                .withName(personsByIDTpRepar)
                .withNumberOfPartitions(personsByIDTpPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(perQueueTime, queueDelay, PER_QUEUE_TAG);
                    return value;
                });

        final KStream<Long, Event> auction = ksMap.get("Branch-auctions")
                .selectKey((key, value) -> {
                    final long procLat = System.nanoTime() - value.startProcTsNano();
                    StreamsUtils.appendLat(aucProcLat, procLat, AUC_PROC_TAG);
                    return value.newAuction.seller;
                })
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(aucBySellerIDTpRepar)
                        .withNumberOfPartitions(aucBySellerIDTpPar))
                .mapValues((key, value) -> {
                    value.setStartProcTsNano(System.nanoTime());
                    final long queueDelay = Instant.now().toEpochMilli() - value.injTsMs();
                    StreamsUtils.appendLat(aucQueueTime, queueDelay, AUC_QUEUE_TAG);
                    return value;
                });

        final long windowSizeMs = 10 * 1000;
        final JoinWindows jw = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5));
        final WindowBytesStoreSupplier auctionStoreSupplier = Stores.inMemoryWindowStore(
                "auction-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);
        final WindowBytesStoreSupplier personStoreSupplier = Stores.inMemoryWindowStore(
                "person-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()),
                Duration.ofMillis(jw.size()), true);
        final StreamJoined<Long, Event, Event> sj = StreamJoined
                .<Long, Event, Event>with(auctionStoreSupplier, personStoreSupplier)
                .withKeySerde(Serdes.Long())
                .withValueSerde(eSerde)
                .withOtherValueSerde(eSerde)
                .withLoggingEnabled(new HashMap<>());
        auction.join(person, (left, right) -> {
            final long startExecNano;
            if (left.startProcTsNano() == 0) {
                startExecNano = right.startProcTsNano();
            } else if (right.startProcTsNano() == 0) {
                startExecNano = left.startProcTsNano();
            } else {
                startExecNano = Math.min(left.startProcTsNano(), right.startProcTsNano());
            }
            assert startExecNano != 0;
            final long ts = right.newPerson.dateTime;
            final long windowStart = (Math.max(0, ts - windowSizeMs + windowSizeMs) / windowSizeMs) * windowSizeMs;
            final PersonTime pt = new PersonTime(right.newPerson.id, right.newPerson.name, windowStart);
            pt.setStartProcTsNano(startExecNano);
            return pt;
        }, jw, sj)
                .transformValues(lcts, Named.as("latency-measure"))
                .to(outTp, Produced.with(Serdes.Long(), ptSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
                disableCache, disableBatching, producerBatchSize);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q8");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
                disableCache, disableBatching, producerBatchSize);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "q8");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
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
