package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.SumAndCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class WindowedAvg implements NexmarkQuery {

    @Override
    public StreamsBuilder getStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        JSONPOJOSerde<Event> serde = new JSONPOJOSerde<Event>(){};
        KStream<String, Event> inputs = builder.stream("nexmark-input", Consumed.with(Serdes.String(), serde)
                .withTimestampExtractor(new JSONTimestampExtractor()));
        inputs.filter((key, value) -> value.type == Event.Type.BID)
                .groupBy((key, value) -> value.bid.auction)
                .aggregate(
                        ()->new SumAndCount(0, 0),
                        (key, value, aggregate) -> new SumAndCount(aggregate.sum + value.bid.price, aggregate.count+1)
                )
                .mapValues((value) -> (double)value.sum / (double)value.count)
                .toStream()
                .to("windowedavg-out", Produced.with(Serdes.Long(), Serdes.Double()));
        return builder;
    }

    @Override
    public Properties getProperties(String bootstrapServer) {
        Properties props = StreamsUtils.getStreamsConfig(bootstrapServer);
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "windowedAvg");
        return props;
    }
}
