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
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.io.FileInputStream;
import java.io.IOException;

public class Query1 implements NexmarkQuery {
    public CountAction<String, Event> input;
    public LatencyCount<String, Event> latCount;

    public Query1() {
        input = new CountAction<>();
        latCount = new LatencyCount<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);
        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic np = new NewTopic(outTp, numPar, (short) 3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));

        Serde<Event> eSerde;
        StreamsBuilder builder = new StreamsBuilder();
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

        final KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));

        inputs.peek(input).filter((key, value) -> (value != null && value.etype == Event.EType.BID))
                .mapValues(value -> {
                    Bid b = value.bid;
                    Event e = new Event(
                            new Bid(b.auction, b.bidder, (b.price * 89) / 100, b.channel, b.url, b.dateTime, b.extra));
                    return e;
                }).peek(latCount).to(outTp, Produced.valueSerde(eSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer, int duration) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer, duration);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q1");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "nexmark-q1-client");
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
    public List<Long> getRecordE2ELatency() {
        return latCount.GetLatencies();
    }
}
