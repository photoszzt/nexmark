package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.NameCityStateId;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class Query3 implements NexmarkQuery {
    public CountAction<String, Event> caInput;
    public CountAction<Long, NameCityStateId> caOutput;

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic np = new NewTopic("nexmark-q3", 1, (short)3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(np));

        StreamsBuilder builder = new StreamsBuilder();

        caInput = new CountAction<>();
        caOutput = new CountAction<>();

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(),
                new JSONPOJOSerde<Event>(){}).withTimestampExtractor(new JSONTimestampExtractor()));

        KeyValueBytesStoreSupplier auctionsBySellerIdKVStoreSupplier = Stores.inMemoryKeyValueStore("auctionBySellerIdKV");
        KTable<Long, Event> auctionsBySellerId = inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.AUCTION)
                .filter((key, value) -> value.newAuction.category == 10)
                .selectKey((key, value) -> value.newAuction.seller)
                .toTable(Named.as("auctionBySellerIdTab"),
                        Materialized.<Long, Event>as(auctionsBySellerIdKVStoreSupplier)
                                .withLoggingEnabled(new HashMap<>())
                                .withCachingEnabled()
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JSONPOJOSerde<Event>(){}));

        KeyValueBytesStoreSupplier personsByIdKVStoreSupplier = Stores.inMemoryKeyValueStore("personsByIdKV");
        KTable<Long, Event> personsById = inputs.filter((key, value) -> value.etype == Event.Type.PERSON)
                .filter((key, value) -> value.newPerson.state.equals("OR") || value.newPerson.state.equals("ID") ||
                        value.newPerson.state.equals("CA"))
                .selectKey((key, value) -> value.newPerson.id)
                .toTable(Named.as("personsByIdTab"),
                        Materialized.<Long, Event>as(personsByIdKVStoreSupplier)
                                .withLoggingEnabled(new HashMap<>())
                                .withCachingEnabled()
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JSONPOJOSerde<Event>(){}));

        auctionsBySellerId
                .join(personsById, (leftValue, rightValue) -> new NameCityStateId(rightValue.newPerson.name,
                        rightValue.newPerson.city,
                        rightValue.newPerson.state,
                        rightValue.newPerson.id))
                .toStream()
                .peek(caOutput)
                .to("nexmark-q3", Produced.with(Serdes.Long(),
                        new JSONPOJOSerde<NameCityStateId>() {}));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q3");
        return props;
    }
}
