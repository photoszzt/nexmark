package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.NameCityStateId;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;

public class Query3 implements NexmarkQuery {

    private Map<String, CountAction> caMap;

    public Query3() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) {
        int numPartition = 5;
        short repartitionFactor = 3;
        NewTopic out = new NewTopic("nexmark-q3-out", numPartition, repartitionFactor);
        NewTopic auctionBySellerIdTabPar = new NewTopic("nexmark-q3-auctionBySellerIdTab-repartition", numPartition, repartitionFactor);
        NewTopic persionsByIdTabPar = new NewTopic("nexmakr-q3-personsByIdTab-repartition", numPartition, repartitionFactor);
        List<NewTopic> nps = new ArrayList<NewTopic>(3);
        nps.add(out);
        nps.add(auctionBySellerIdTabPar);
        nps.add(persionsByIdTabPar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        StreamsBuilder builder = new StreamsBuilder();

        CountAction<String, Event> caInput = new CountAction<String, Event>();
        CountAction<Long, NameCityStateId> caOutput = new CountAction<Long, NameCityStateId>();
        caMap.put("caInput", caInput);
        caMap.put("caOutput", caOutput);

        Serde<Event> eSerde;
        Serde<NameCityStateId> ncsiSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<NameCityStateId> ncsiSerdeJSON = new JSONPOJOSerde<>();
            ncsiSerdeJSON.setClass(NameCityStateId.class);
            ncsiSerde = ncsiSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<NameCityStateId> ncsiSerdeMsgp = new MsgpPOJOSerde<>();
            ncsiSerdeMsgp.setClass(NameCityStateId.class);
            ncsiSerde = ncsiSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));

        KeyValueBytesStoreSupplier auctionsBySellerIdKVStoreSupplier = Stores.inMemoryKeyValueStore("auctionBySellerIdKV");
        KTable<Long, Event> auctionsBySellerId = inputs.peek(caInput)
                .filter((key, value) -> value.etype == Event.EType.AUCTION && value.newAuction.category == 10)
                .selectKey((key, value) -> value.newAuction.seller)
                .toTable(Named.as("auctionBySellerIdTab"),
                        Materialized.<Long, Event>as(auctionsBySellerIdKVStoreSupplier)
                                .withLoggingEnabled(new HashMap<>())
                                .withCachingEnabled()
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        KeyValueBytesStoreSupplier personsByIdKVStoreSupplier = Stores.inMemoryKeyValueStore("personsByIdKV");
        KTable<Long, Event> personsById = inputs
                .filter((key, value) -> value.etype == Event.EType.PERSON)
                .filter((key, value) -> value.newPerson.state.equals("OR") || value.newPerson.state.equals("ID") ||
                        value.newPerson.state.equals("CA"))
                .selectKey((key, value) -> value.newPerson.id)
                .toTable(Named.as("personsByIdTab"),
                        Materialized.<Long, Event>as(personsByIdKVStoreSupplier)
                                .withLoggingEnabled(new HashMap<>())
                                .withCachingEnabled()
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        auctionsBySellerId
                .join(personsById, (leftValue, rightValue) -> new NameCityStateId(rightValue.newPerson.name,
                        rightValue.newPerson.city,
                        rightValue.newPerson.state,
                        rightValue.newPerson.id))
                .toStream()
                .peek(caOutput)
                .to("nexmark-q3-out", Produced.with(Serdes.Long(), ncsiSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q3");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
