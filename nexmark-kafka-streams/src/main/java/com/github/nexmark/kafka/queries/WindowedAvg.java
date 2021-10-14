package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.SumAndCount;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class WindowedAvg implements NexmarkQuery {
    public CountAction<String, Event> caInput;
    public CountAction<Windowed<Long>, Double> caOutput;

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer) {
        NewTopic out = new NewTopic("windowedavg-out", 1, (short)3);
        NewTopic storeTp = new NewTopic("windowedavg-agg-store", 1, (short)3);
        ArrayList<NewTopic> newTps = new ArrayList<>(2);
        newTps.add(out);
        newTps.add(storeTp);
        StreamsUtils.createTopic(bootstrapServer, newTps);

        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>(){};
        KStream<String, Event> inputs = builder.stream("nexmark_src", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        caInput = new CountAction<>();
        caOutput = new CountAction<>();
        TimeWindows tw = TimeWindows.of(Duration.ofSeconds(10));
        WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore("windowedavg-agg-store",
                Duration.ofMillis(tw.gracePeriodMs()+tw.size()), Duration.ofMillis(tw.size()), false);
        
        inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.BID)
                .groupBy((key, value) -> value.bid.auction)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(
                        ()->new SumAndCount(0, 0),
                        (key, value, aggregate) -> new SumAndCount(aggregate.sum + value.bid.price, aggregate.count+1),
                        Named.as("windowedavg-agg"), Materialized.<Long, SumAndCount>as(storeSupplier)
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(new JSONPOJOSerde<SumAndCount>(){})
                                .withCachingEnabled()
                                .withLoggingEnabled(new HashMap<>())
                )
                .mapValues((value) -> (double)value.sum / (double)value.count)
                .toStream()
                .peek(caOutput)
                .to("windowedavg-out", Produced.with(new JSONPOJOSerde<>(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "windowedAvg");
        return props;
    }
}
