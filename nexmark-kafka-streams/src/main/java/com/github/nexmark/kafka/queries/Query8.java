package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PersonTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.*;

public class Query8 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query8() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        int numPartition = 5;
        short replicationFactor = 3;
        List<NewTopic> nps = new ArrayList<>(3);
        NewTopic q8 = new NewTopic("nexmark-q8-out", numPartition, replicationFactor);
        NewTopic personRepar = new NewTopic("nexmark-q8-person-repartition", numPartition, replicationFactor);
        NewTopic auctionRepar = new NewTopic("nexmark-q8-auction-repartition", numPartition, replicationFactor);
        nps.add(q8);
        nps.add(personRepar);
        nps.add(auctionRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        CountAction<String, Event> caInput = new CountAction<String, Event>();
        CountAction<Long, PersonTime> caOutput = new CountAction<Long, PersonTime>();
        caMap.put("caInput", caInput);
        caMap.put("caOutput", caOutput);

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>();
        serde.setClass(Event.class);
        JSONPOJOSerde<PersonTime> ptSerde = new JSONPOJOSerde<>();
        ptSerde.setClass(PersonTime.class);

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new EventTimestampExtractor()));

        KStream<Long, Event> person = inputs
                .peek(caInput)
                .filter((key, value) -> value.etype == Event.Type.PERSON)
                .selectKey((key, value) -> value.newPerson.id)
                .repartition(Repartitioned.with(Serdes.Long(), serde)
                        .withName("person-repartition")
                        .withNumberOfPartitions(numPartition));

        KStream<Long, Event> auction = inputs.filter((key, value) -> value.etype == Event.Type.AUCTION)
                .selectKey((key, value) -> value.newAuction.seller)
                .repartition(Repartitioned.with(Serdes.Long(), serde)
                        .withName("auction-repartition")
                        .withNumberOfPartitions(numPartition));

        JoinWindows jw = JoinWindows.of(Duration.ofSeconds(10));
        WindowBytesStoreSupplier auctionStoreSupplier = Stores.inMemoryWindowStore(
                "auction-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()), Duration.ofMillis(jw.size()), true
        );
        WindowBytesStoreSupplier personStoreSupplier = Stores.inMemoryWindowStore(
                "person-join-store", Duration.ofMillis(jw.size() + jw.gracePeriodMs()), Duration.ofMillis(jw.size()), true
        );
        auction.join(person, new ValueJoiner<Event, Event, PersonTime>() {
                            @Override
                            public PersonTime apply(Event event, Event event2) {
                                return new PersonTime(event2.newPerson.id, event2.newPerson.name, 0);
                            }
                        }, jw, StreamJoined.<Long, Event, Event>with(auctionStoreSupplier, personStoreSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(serde)
                                .withOtherValueSerde(serde)
                                .withLoggingEnabled(new HashMap<>())
                                .withStoreName("auction-join-persion-store")
                )
                .peek(caOutput)
                .to("nexmark-q8-out", Produced.with(Serdes.Long(), ptSerde));
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
