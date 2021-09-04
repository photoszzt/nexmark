package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.AucIdCategory;
import com.github.nexmark.kafka.model.AuctionBid;
import com.github.nexmark.kafka.model.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;

import java.util.Properties;

public class Query4 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>();
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), serde));
        KTable<Long, Event> bid = inputs.filter((key, value) -> value.type == Event.Type.BID)
                .map((key, value) -> KeyValue.pair(value.bid.auction, value)).toTable();
        KTable<Long, Event> auction = inputs.filter((key, value) -> value.type == Event.Type.AUCTION)
                .map((key, value) -> KeyValue.pair(value.newAuction.id, value)).toTable();
        auction.join(bid, (leftValue, rightValue) -> {
            return new AuctionBid(rightValue.bid.dateTime, leftValue.newAuction.dateTime, 
            leftValue.newAuction.expires, rightValue.bid.price, leftValue.newAuction.category);
        }).groupBy((key, value) -> KeyValue.pair(new AucIdCategory(key, value.aucCategory), value));
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q4");
        return props;
    }
}
