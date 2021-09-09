package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PriceTime;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Properties;
import java.util.Set;

public class Query7 implements NexmarkQuery {
    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>() {
        };
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        KStream<Long, Event> bid = inputs.filter((key, value) -> value.type == Event.Type.BID)
                .selectKey((key, value) -> value.bid.price);
        KTable<Windowed<Long>, PriceTime> maxBid = bid.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(
                        () -> new PriceTime(0, Instant.MIN),
                        (key, value, aggregate) -> {
                            if (value.bid.price > aggregate.price) {
                                return new PriceTime(value.bid.price,
                                        value.bid.dateTime);
                            } else {
                                return aggregate;
                            }
                        }, Materialized.as("max-bid"));
        bid.transformValues(new ValueTransformerWithKeySupplier<Long, Event, BidAndMax>() {
            public ValueTransformerWithKey get() {
                return new ValueTransformerWithKey() {
                    private ProcessorContext context;
                    private KeyValueStore stateStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.stateStore = (KeyValueStore) context.getStateStore("max-bid");
                    }

                    @Override
                    public Object transform(Object readOnlyKey, Object value) {
                        Event event = (Event) value;
                        PriceTime pt = (PriceTime) this.stateStore.get(readOnlyKey);
                        if (pt != null) {
                            return new BidAndMax(event.bid.auction, event.bid.price,
                                    event.bid.bidder, event.bid.dateTime, event.bid.extra,
                                    pt.dateTime);
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        }, "max-bid")
                .filter((key, value) -> {
                    Instant lb = value.maxDateTime.minus(10, ChronoUnit.SECONDS);
                    return value.dateTime.compareTo(lb) >= 0 && value.dateTime.compareTo(value.maxDateTime) <= 0;
                }).to("nexmark-q7", Produced.with(Serdes.Long(), new JSONPOJOSerde<BidAndMax>() {
        }));
        return builder;
    }

    @Override
    public Properties getProperties() {
        Properties props = StreamsUtils.getStreamsConfig();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q7");
        return props;
    }
}
