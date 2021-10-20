package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AucIdCategory;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Query4 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query4() {
        caMap = new HashMap<>();
    }
    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Event> eSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }

        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde)
                        .withTimestampExtractor(new EventTimestampExtractor()));

        KeyValueBytesStoreSupplier bidKVSupplier = Stores.inMemoryKeyValueStore("bidTab");
        KTable<Long, Event> bid = inputs
                .filter((key, value) -> value.etype == Event.Type.BID)
                .selectKey((key, value) -> value.bid.auction)
                .toTable(Named.as("bidTabNode"),
                        Materialized.<Long, Event>as(bidKVSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        KeyValueBytesStoreSupplier auctionKVSupplier = Stores.inMemoryKeyValueStore("auctionTab");
        KTable<Long, Event> auction = inputs
                .filter((key, value) -> value.etype == Event.Type.AUCTION)
                .selectKey((key, value) -> value.newAuction.id)
                .toTable(Named.as("auctionTabNode"),
                        Materialized.<Long, Event>as(auctionKVSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(eSerde));

        auction.join(bid, (leftValue, rightValue) -> new AuctionBid(rightValue.bid.dateTime,
                        leftValue.newAuction.dateTime, leftValue.newAuction.expires,
                        rightValue.bid.price, leftValue.newAuction.category))
                .filter((key, value) -> value.bidDateTime.compareTo(value.aucDateTime) >= 0
                        && value.bidDateTime.compareTo(value.aucExpires) <= 0
                )
                .groupBy((key, value) -> KeyValue.pair(new AucIdCategory(key, value.aucCategory), value));
        // TODO: aggregate is not done
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q4");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
