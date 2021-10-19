package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Query2 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query2() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic np = new NewTopic("nexmark-q2", 1, (short) 3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));
        StreamsBuilder builder = new StreamsBuilder();

        CountAction<String, Event> caInput = new CountAction<String, Event>();
        CountAction<String, Event> caOutput = new CountAction<String, Event>();
        caMap.put("caInput", caInput);
        caMap.put("caOutput", caOutput);
        
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<>();
        serde.setClass(Event.class);

        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), serde)
                        .withTimestampExtractor(new JSONTimestampExtractor()));
        inputs.peek(caInput)
                .filter((key, value) -> value.etype == Event.Type.BID && value.bid.auction % 123 == 0)
                .peek(caOutput)
                .to("nexmark-q2-out", Produced.valueSerde(serde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q2");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
