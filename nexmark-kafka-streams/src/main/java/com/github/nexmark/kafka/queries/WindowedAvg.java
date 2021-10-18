package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.SumAndCount;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WindowedAvg implements NexmarkQuery {
    public Map<String, CountAction> caMap;

    public WindowedAvg() {
        this.caMap = new HashMap<>();
        caMap.put("caInput", new CountAction<String, Event>());
        caMap.put("caOutput", new CountAction<Windowed<Long>, Double>());
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic out = new NewTopic("windowedavg-out", 1, (short) 3);
        NewTopic storeTp = new NewTopic("windowedavg-agg-store", 1, (short) 3);
        ArrayList<NewTopic> newTps = new ArrayList<>(2);
        newTps.add(out);
        newTps.add(storeTp);
        StreamsUtils.createTopic(bootstrapServer, newTps);

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>();
        serde.setClass(Event.class);

        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        CountAction<String, Event> caInput = caMap.get("caInput");
        CountAction<Windowed<Long>, Double> caOutput = caMap.get("caOutput");
        TimeWindows tw = TimeWindows.of(Duration.ofSeconds(10));
        WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore("windowedavg-agg-store",
                Duration.ofMillis(tw.gracePeriodMs() + tw.size()), Duration.ofMillis(tw.size()), false);

        JSONPOJOSerde<SumAndCount> scSerde = new JSONPOJOSerde<SumAndCount>();
        scSerde.setClass(SumAndCount.class);

        Serde<Long> longSerde = Serdes.Long();
        TimeWindowedSerializer<Long> longWindowSerializer = new TimeWindowedSerializer<Long>(longSerde.serializer());
        TimeWindowedDeserializer<Long> longWindowDeserializer = new TimeWindowedDeserializer<Long>(longSerde.deserializer(), tw.sizeMs);
        Serde<Windowed<Long>> windowedLongSerde = Serdes.serdeFrom(longWindowSerializer, longWindowDeserializer);

        inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.BID)
                .selectKey((key, value) -> value.bid.auction)
                .repartition(Repartitioned.with(Serdes.Long(), serde).withNumberOfPartitions(5).withName("groupby-repartition-node"))
                .groupByKey(Grouped.with(Serdes.Long(), serde))
                .windowedBy(tw)
                .aggregate(
                        () -> new SumAndCount(0, 0),
                        (key, value, aggregate) -> new SumAndCount(aggregate.sum + value.bid.price, aggregate.count + 1),
                        Named.as("windowedavg-agg"), Materialized.<Long, SumAndCount>as(storeSupplier)
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                                .withValueSerde(scSerde)
                                .withKeySerde(Serdes.Long())
                )
                .mapValues((key, value) -> (double) value.sum / (double) value.count)
                .toStream()
                .peek(caOutput)
                .to("windowedavg-out", Produced.with(windowedLongSerde, Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "windowedAvg");
        return props;
    }

    @Override
    public Map<String, CountAction> getCountActionMap() {
        return caMap;
    }
}
