package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Bid;
import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;

import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query1 implements NexmarkQuery {
    // public CountAction<Event> input;
    public LatencyCount<String, Event> latCount;

    public Query1(final File statsDir) {
        // input = new CountAction<>();
        final String tag = "q1_sink_ets";
        final String fileName = statsDir + File.separator + tag;

        latCount = new LatencyCount<>(tag, fileName);
    }

    @Override
    public StreamsBuilder getStreamBuilder(final String bootstrapServer, final String serde, final String configFile) throws IOException {
        final Properties prop = new Properties();
        final FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);
        final String outTp = prop.getProperty("out.name");
        final int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        final NewTopic np = new NewTopic(outTp, numPar, REPLICATION_FACTOR);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));

        final Serde<Event> eSerde;
        final StreamsBuilder builder = new StreamsBuilder();
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

        final KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
            .withTimestampExtractor(new EventTimestampExtractor()));

        inputs.filter((key, value) -> value != null && value.etype == Event.EType.BID)
            .mapValues(value -> {
                final Bid b = value.bid;
                return new Event(
                    new Bid(b.auction, b.bidder, (b.price * 89) / 100, b.channel, b.url, b.dateTime, b.extra),
                    Instant.now().toEpochMilli());
            }).peek(latCount, Named.as("measure-latency")).to(outTp, Produced.valueSerde(eSerde));
        return builder;
    }

    @Override
    public Properties getExactlyOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching, producerBatchSize);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q1");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "q1-client");
        return props;
    }

    @Override
    public Properties getAtLeastOnceProperties(final String bootstrapServer, final int duration, final int flushms,
                                               final boolean disableCache, final boolean disableBatching,
                                               final int producerBatchSize) {
        final Properties props = StreamsUtils.getAtLeastOnceStreamsConfig(bootstrapServer, duration, flushms,
            disableCache, disableBatching, producerBatchSize);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q1");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "q1-client");
        return props;
    }

    // @Override
    // public long getInputCount() {
    // return input.GetProcessedRecords();
    // }

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

    // @Override
    // public void printRemainingStats() {
    // latCount.printRemainingStats();
    // }

}
