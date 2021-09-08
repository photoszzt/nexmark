package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.Person;
import com.github.nexmark.kafka.model.PersonIdName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class Query8 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>() {
        };
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<PersonIdName, Event> person = inputs
                .filter((key, value) -> value.type == Event.Type.PERSON)
                .selectKey((key, value) ->
                        new PersonIdName(value.newPerson.id,
                                value.newPerson.name));
        KStream<Long, Event> auction = inputs.filter((key, value) -> value.type == Event.Type.AUCTION)
                .selectKey((key, value) -> value.newAuction.seller);
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q8");
        return props;
    }
}
