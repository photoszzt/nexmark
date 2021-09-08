package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.NameCityStateId;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class Query3 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(),
                new JSONPOJOSerde<Event>(){}).withTimestampExtractor(new JSONTimestampExtractor()));
        KTable<Long, Event> auctionsBySellerId = inputs.filter((key, value) -> value.type == Event.Type.AUCTION)
                .filter((key, value) -> value.newAuction.category == 10)
                .selectKey((key, value) -> value.newAuction.seller).toTable();
        KTable<Long, Event> personsById = inputs.filter((key, value) -> value.type == Event.Type.PERSON)
                .filter((key, value) -> value.newPerson.state.equals("OR") || value.newPerson.state.equals("ID") ||
                        value.newPerson.state.equals("CA"))
                .selectKey((key, value) -> value.newPerson.id).toTable();
        KTable<Long, NameCityStateId> q3Out = auctionsBySellerId.join(personsById, (leftValue, rightValue) -> {
            return new NameCityStateId(rightValue.newPerson.name, rightValue.newPerson.city, rightValue.newPerson.state, rightValue.newPerson.id);
        });

        q3Out.toStream().to("nexmark-q3", Produced.with(Serdes.Long(), new JSONPOJOSerde<NameCityStateId>() {}));
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q3");
        return props;
    }
}
