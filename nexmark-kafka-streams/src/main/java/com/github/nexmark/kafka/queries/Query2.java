package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Named;

import java.io.IOException;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Properties;
import static com.github.nexmark.kafka.queries.Constants.REPLICATION_FACTOR;

public class Query2 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCount<String, Event> latCount;

    public Query2() {
        input = new CountAction<>();
        latCount = new LatencyCount<>("q2_sink_ets");
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);
        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic np = new NewTopic(outTp, numPar, REPLICATION_FACTOR);

        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Event> eSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        input = new CountAction<>();

        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde)
                        .withTimestampExtractor(new EventTimestampExtractor()));
        inputs.peek(input)
                .filter((key, value) -> value != null && value.etype == Event.EType.BID && value.bid.auction % 123 == 0)
                .peek(latCount, Named.as("measure-latency"))
                .to(outTp, Produced.valueSerde(eSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer, int duration, int flushms) {
        Properties props = StreamsUtils.getExactlyOnceStreamsConfig(bootstrapServer, duration, flushms);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "q2");
        return props;
    }

    @Override
    public long getInputCount() {
        return input.GetProcessedRecords();
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
    public void printRemainingStats() {
        latCount.printRemainingStats();
    }
}
