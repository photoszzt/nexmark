package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PriceTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Query7 implements NexmarkQuery {
    private Map<String, CountAction> caMap;
    public CountAction<String, Event> caInput;
    public CountAction<Long, BidAndMax> caOutput;

    public Query7() {
        caMap = new HashMap<>();
        caMap.put("caInput", new CountAction<String, Event>());
        caMap.put("caOutput", new CountAction<Long, BidAndMax>());
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic q7 = new NewTopic("nexmark-q7", 1, (short) 3);
        StreamsUtils.createTopic(bootstrapServer, Collections.singleton(q7));

        CountAction<String, Event> caInput = caMap.get("caInput");
        CountAction<Long, BidAndMax> caOutput = caMap.get("caOutput");

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>();
        serde.setClass(Event.class);

        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), serde).withTimestampExtractor(new JSONTimestampExtractor()));

        KStream<Long, Event> bid = inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.BID)
                .selectKey((key, value) -> value.bid.price);

        TimeWindows tw = TimeWindows.of(Duration.ofSeconds(10));
        WindowBytesStoreSupplier maxBidWinStoreSupplier = Stores.inMemoryWindowStore(
                "max-bid-tab", Duration.ofMillis(tw.size() + tw.gracePeriodMs()), Duration.ofMillis(tw.size()), true);
        JSONPOJOSerde<PriceTime> ptSerde = new JSONPOJOSerde<>();
        ptSerde.setClass(PriceTime.class);

        bid.groupByKey(Grouped.with(Serdes.Long(), serde))
                .windowedBy(tw)
                .aggregate(() -> new PriceTime(0, Instant.MIN), (key, value, aggregate) -> {
                    if (value.bid.price > aggregate.price) {
                        return new PriceTime(value.bid.price, value.bid.dateTime);
                    } else {
                        return aggregate;
                    }
                }, Materialized.<Long, PriceTime>as(maxBidWinStoreSupplier)
                        .withCachingEnabled()
                        .withLoggingEnabled(new HashMap<>())
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(ptSerde));

        JSONPOJOSerde<BidAndMax> bmSerde = new JSONPOJOSerde<>();
        bmSerde.setClass(BidAndMax.class);

        bid.transformValues(new ValueTransformerWithKeySupplier<Long, Event, BidAndMax>() {
                    public ValueTransformerWithKey<Long, Event, BidAndMax> get() {
                        return new ValueTransformerWithKey<Long, Event, BidAndMax>() {
                            private KeyValueStore<Long, PriceTime> stateStore;

                            @Override
                            public void init(ProcessorContext context) {
                                this.stateStore = (KeyValueStore<Long, PriceTime>) context.getStateStore("max-bid");
                            }

                            @Override
                            public BidAndMax transform(Long readOnlyKey, Event value) {
                                Event event = (Event) value;
                                PriceTime pt = (PriceTime) this.stateStore.get(readOnlyKey);
                                if (pt != null) {
                                    return new BidAndMax(event.bid.auction, event.bid.price, event.bid.bidder,
                                            event.bid.dateTime, event.bid.extra, pt.dateTime);
                                } else {
                                    return null;
                                }
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                }, "max-bid-tab")
                .filter((key, value) -> {
                    Instant lb = value.maxDateTime.minus(10, ChronoUnit.SECONDS);
                    return value.dateTime.compareTo(lb) >= 0 && value.dateTime.compareTo(value.maxDateTime) <= 0;
                }).peek(caOutput).to("nexmark-q7", Produced.with(Serdes.Long(), bmSerde));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "nexmark-q7");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
