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
    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde, String configFile) {
        int numPartition = 5;
        short replicationFactor = 3;
        NewTopic out = new NewTopic("windowedavg-out", numPartition, replicationFactor);
        NewTopic groupByRepar = new NewTopic("windowedAvg-groupby-repar-repartition", numPartition, replicationFactor);

        ArrayList<NewTopic> newTps = new ArrayList<>(2);
        newTps.add(out);
        newTps.add(groupByRepar);
        StreamsUtils.createTopic(bootstrapServer, newTps);

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
        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), eSerde)
                .withTimestampExtractor(new EventTimestampExtractor()));

        TimeWindows tw = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10));
        WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore("windowedavg-agg-store",
                Duration.ofMillis(tw.gracePeriodMs() + tw.size()), Duration.ofMillis(tw.size()), false);

        JSONPOJOSerde<SumAndCount> scSerde = new JSONPOJOSerde<SumAndCount>();
        scSerde.setClass(SumAndCount.class);

        Serde<Long> longSerde = Serdes.Long();
        TimeWindowedSerializer<Long> longWindowSerializer = new TimeWindowedSerializer<Long>(longSerde.serializer());
        TimeWindowedDeserializer<Long> longWindowDeserializer = new TimeWindowedDeserializer<Long>(longSerde.deserializer(), tw.sizeMs);
        Serde<Windowed<Long>> windowedLongSerde = Serdes.serdeFrom(longWindowSerializer, longWindowDeserializer);

        inputs.filter((key, value) -> value.etype == Event.EType.BID)
                .selectKey((key, value) -> value.bid.auction)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde).withNumberOfPartitions(numPartition).withName("groupby-repar"))
                .groupByKey(Grouped.with(Serdes.Long(), eSerde))
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
    public long getInputCount() {
        return 0;
    }
}
