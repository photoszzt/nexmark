package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PersonTime;
import com.github.nexmark.kafka.model.Event.EType;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.*;
import java.io.IOException;
import java.io.FileInputStream;

public class Query8 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query8() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) throws IOException{
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream(configFile);
        prop.load(fis);

        String outTp = prop.getProperty("out.name");
        int numPar = Integer.parseInt(prop.getProperty("out.numPar"));
        NewTopic out = new NewTopic(outTp, numPar, (short) 3);

        String aucBySellerIDTp = prop.getProperty("aucBySellerIDTp.name");
        int aucBySellerIDTpPar = Integer.parseInt(prop.getProperty("aucBySellerIDTp.numPar"));
        NewTopic auctionRepar = new NewTopic(aucBySellerIDTp, aucBySellerIDTpPar, (short) 3);

        String personsByIDTp = prop.getProperty("personsByIDTp.name");
        int personsByIDTpPar = Integer.parseInt(prop.getProperty("personsByIDTp.numPar"));
        NewTopic personRepar = new NewTopic(personsByIDTp, personsByIDTpPar, (short) 3);

        List<NewTopic> nps = new ArrayList<>(3);
        nps.add(out);
        nps.add(personRepar);
        nps.add(auctionRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        CountAction<String, Event> caInput = new CountAction<String, Event>();
        CountAction<Long, PersonTime> caOutput = new CountAction<Long, PersonTime>();
        caMap.put("caInput", caInput);
        caMap.put("caOutput", caOutput);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Event> eSerde;
        Serde<PersonTime> ptSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<PersonTime> ptSerdeJSON = new JSONPOJOSerde<>();
            ptSerdeJSON.setClass(PersonTime.class);
            ptSerde = ptSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<PersonTime> ptSerdeMsgp = new MsgpPOJOSerde<>();
            ptSerdeMsgp.setClass(PersonTime.class);
            ptSerde = ptSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));

        KStream<Long, Event> person = inputs
                .peek(caInput)
                .filter((key, value) -> value.etype == Event.EType.PERSON)
                .selectKey((key, value) -> value.newPerson.id)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(personsByIDTp)
                        .withNumberOfPartitions(personsByIDTpPar));

        KStream<Long, Event> auction = inputs.filter((key, value) -> value.etype == Event.EType.AUCTION)
                .selectKey((key, value) -> value.newAuction.seller)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName(aucBySellerIDTp)
                        .withNumberOfPartitions(aucBySellerIDTpPar));

        JoinWindows jw = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10));
        WindowBytesStoreSupplier auctionStoreSupplier = Stores.inMemoryWindowStore(
                "auction-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()), 
                Duration.ofMillis(jw.size()), true
        );
        WindowBytesStoreSupplier personStoreSupplier = Stores.inMemoryWindowStore(
                "person-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()), 
                Duration.ofMillis(jw.size()), true
        );
        auction.join(person, new ValueJoiner<Event, Event, PersonTime>() {
                            @Override
                            public PersonTime apply(Event event, Event event2) {
                                if (event2.etype == EType.PERSON) {
                                    return new PersonTime(event2.newPerson.id, event2.newPerson.name, 0);
                                } else {
                                    return new PersonTime(event.newPerson.id, event.newPerson.name, 0);
                                }
                            }
                        }, jw, StreamJoined.<Long, Event, Event>with(auctionStoreSupplier, personStoreSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde)
                                .withOtherValueSerde(eSerde)
                                .withLoggingEnabled(new HashMap<>())
                )
                .peek(caOutput)
                .to(outTp, Produced.with(Serdes.Long(), ptSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q8");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
