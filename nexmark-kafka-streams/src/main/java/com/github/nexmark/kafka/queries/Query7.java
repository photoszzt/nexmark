package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.BidAndMax;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.PriceTime;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Query7 implements NexmarkQuery {
    private Map<String, CountAction> caMap;

    public Query7() {
        caMap = new HashMap<>();
    }

    @Override
    public StreamsBuilder getStreamBuilder(String bootstrapServer, String serde) {
        int numPartition = 5;
        short replicationFactor = 3;
        List<NewTopic> nps = new ArrayList<>(2);
        NewTopic q7 = new NewTopic("nexmark-q7-out", numPartition, replicationFactor);
        NewTopic bidRepar = new NewTopic("nexmark-q7-bid-repartition", numPartition, replicationFactor);
        nps.add(q7);
        nps.add(bidRepar);
        StreamsUtils.createTopic(bootstrapServer, nps);

        CountAction<String, Event> caInput = new CountAction<String, Event>();
        CountAction<Long, BidAndMax> caOutput = new CountAction<Long, BidAndMax>();
        caMap.put("caInput", caInput);
        caMap.put("caOutput", caOutput);

        Serde<Event> eSerde;
        Serde<PriceTime> ptSerde;
        Serde<BidAndMax> bmSerde;
        if (serde.equals("json")) {
            JSONPOJOSerde<Event> eSerdeJSON = new JSONPOJOSerde<Event>();
            eSerdeJSON.setClass(Event.class);
            eSerde = eSerdeJSON;

            JSONPOJOSerde<PriceTime> ptSerdeJSON = new JSONPOJOSerde<>();
            ptSerdeJSON.setClass(PriceTime.class);
            ptSerde = ptSerdeJSON;

            JSONPOJOSerde<BidAndMax> bmSerdeJSON = new JSONPOJOSerde<>();
            bmSerdeJSON.setClass(BidAndMax.class);
            bmSerde = bmSerdeJSON;
        } else if (serde.equals("msgp")) {
            MsgpPOJOSerde<Event> eSerdeMsgp = new MsgpPOJOSerde<>();
            eSerdeMsgp.setClass(Event.class);
            eSerde = eSerdeMsgp;

            MsgpPOJOSerde<PriceTime> ptSerdeMsgp = new MsgpPOJOSerde<>();
            ptSerdeMsgp.setClass(PriceTime.class);
            ptSerde = ptSerdeMsgp;

            MsgpPOJOSerde<BidAndMax> bmSerdeMsgp = new MsgpPOJOSerde<>();
            bmSerdeMsgp.setClass(BidAndMax.class);
            bmSerde = bmSerdeMsgp;
        } else {
            throw new RuntimeException("serde expects to be either json or msgp; Got " + serde);
        }
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Event> inputs = builder.stream("nexmark_src",
                Consumed.with(Serdes.String(), eSerde).withTimestampExtractor(new EventTimestampExtractor()));

        int numberOfPartition = 5;
        KStream<Long, Event> bid = inputs.peek(caInput).filter((key, value) -> value.etype == Event.Type.BID)
                .selectKey((key, value) -> value.bid.price)
                .repartition(Repartitioned.with(Serdes.Long(), eSerde)
                        .withName("bid-repartition")
                        .withNumberOfPartitions(numberOfPartition));

        TimeWindows tw = TimeWindows.of(Duration.ofSeconds(10));
        WindowBytesStoreSupplier maxBidWinStoreSupplier = Stores.inMemoryWindowStore(
                "max-bid-tab", Duration.ofMillis(tw.size() + tw.gracePeriodMs()), Duration.ofMillis(tw.size()), false);


        bid.groupByKey(Grouped.with(Serdes.Long(), eSerde))
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


        bid.transform(new TransformerSupplier<Long, Event, KeyValue<Long, BidAndMax>>() {
            @Override
            public Transformer<Long, Event, KeyValue<Long, BidAndMax>> get() {
                return new Transformer<Long, Event, KeyValue<Long, BidAndMax>>() {
                    private TimestampedWindowStore<Long, PriceTime> stateStore;
                    private ProcessorContext pctx;

                    @Override
                    public void init(ProcessorContext processorContext) {
                        this.stateStore = (TimestampedWindowStore<Long, PriceTime>) processorContext.getStateStore("max-bid-tab");
                        this.pctx = processorContext;
                    }

                    @Override
                    public KeyValue<Long, BidAndMax> transform(Long aLong, Event event) {
                        WindowStoreIterator<ValueAndTimestamp<PriceTime>> ptIter = this.stateStore.fetch(aLong, event.bid.dateTime.minusSeconds(10), event.bid.dateTime);
                        while (ptIter.hasNext()) {
                            KeyValue<Long, ValueAndTimestamp<PriceTime>> kv = ptIter.next();
                            if (event.bid.price == kv.value.value().price) {
                                pctx.forward(aLong, new BidAndMax(event.bid.auction, event.bid.price, event.bid.bidder,
                                        event.bid.dateTime, event.bid.extra, kv.value.value().dateTime));
                            }
                        }
                        return null;
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
                }).peek(caOutput).to("nexmark-q7-out", Produced.with(Serdes.Long(), bmSerde));
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
