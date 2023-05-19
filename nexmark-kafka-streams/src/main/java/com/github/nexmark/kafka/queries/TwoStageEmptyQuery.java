package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Named;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.File;

import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class TwoStageEmptyQuery implements NexmarkQuery {
    private final LatencyCount<String, Event> latCount;

    public TwoStageEmptyQuery(final String statsDir) {
        final String tag = "twoStageEmpty_sink_ets";
        final String fileName = statsDir + File.separator + tag;
        latCount = new LatencyCount<>(tag, fileName);
    }

    @Override
    public StreamsBuilder getStreamBuilder(final String bootstrapServer, final String serde, final String configFile) throws IOException {
        final Properties prop = new Properties();
        final FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);
        final String midTp = prop.getProperty("mid1.name");
        final int numParMid = Integer.parseInt(prop.getProperty("mid1.numPar"));
        final NewTopic mid = new NewTopic(midTp, numParMid, REPLICATION_FACTOR);

        final String outTp = prop.getProperty("out.name");
        final int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        final NewTopic out = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        final List<NewTopic> nps = new ArrayList<>(2);
        nps.add(out);
        nps.add(mid);

        StreamsUtils.createTopic(bootstrapServer, nps);
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<Event> eSerde;
        if (serde.equals("json")) {
            final JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;
        } else if (serde.equals("msgp")) {
            final MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }
        builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()))
            .repartition(Repartitioned.with(Serdes.String(), eSerde)
                .withName(midTp).withNumberOfPartitions(numParMid))
            .peek(latCount, Named.as("measure-latency"))
            .to(outTp, Produced.with(Serdes.String(), eSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration,
                                               final int flushms, final boolean disableCache,
                                               final boolean disableBatching, final int producerBatchSize) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching, producerBatchSize);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "two-empty");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "two-empty-client");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching, producerBatchSize);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "two-empty");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "two-empty-client");
        return props;
    }

    @Override
    public void setAfterWarmup() {
        latCount.SetAfterWarmup();
    }

    @Override
    public void printCount() {
        latCount.printCount();
    }

    @Override
    public void outputRemainingStats() {
        latCount.outputRemainingStats();
    }
}