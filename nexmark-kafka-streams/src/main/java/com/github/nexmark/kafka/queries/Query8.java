package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PersonTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Query8 implements NexmarkQuery {
    public CountAction<String, Event> caInput;
    public CountAction<Long, PersonTime> caOutput;

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic q8 = new NewTopic("nexmark-q8", 1, (short)3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(q8));

        caInput = new CountAction<>();
        caOutput = new CountAction<>();

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>() {
        };
        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<Long, Event> person = inputs
                .peek(caInput)
                .filter((key, value) -> value.etype == Event.Type.PERSON)
                .selectKey((key, value) ->
                        value.newPerson.id
                );
        KStream<Long, Event> auction = inputs.filter((key, value) -> value.etype == Event.Type.AUCTION)
                .selectKey((key, value) -> value.newAuction.seller);
        auction.join(person,
                (leftValue, rightValue) -> new PersonTime(rightValue.newPerson.id, rightValue.newPerson.name, 0), JoinWindows.of(Duration.ofSeconds(10)))
                .peek(caOutput)
                .to("nexmark-q8", Produced.with(Serdes.Long(), new JSONPOJOSerde<PersonTime>(){}));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q8");
        return props;
    }
}
