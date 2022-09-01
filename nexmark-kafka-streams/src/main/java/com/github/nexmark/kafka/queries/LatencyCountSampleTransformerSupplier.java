package com.github.nexmark.kafka.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LatencyCountSampleTransformerSupplier<V> implements ValueTransformerSupplier<V, V>{

    List<LatencySampleHolder> holders = new ArrayList<>();
    String tag;
    int numHolders;

    public LatencyCountSampleTransformerSupplier(String tag) {
        this.tag = tag;
    }

    @Override
    public ValueTransformer<V, V> get() {
        LatencySampleHolder holder = new LatencySampleHolder(tag+"-"+numHolders);
        holders.add(holder);
        numHolders += 1;
        return new LatencyCountTransformer<V>(holder);
    }

    public void SetAfterWarmup(){
        for (LatencySampleHolder lh : holders) {
            lh.SetAfterWarmup();
        }
    }

    public void printCount() {
        if (holders.size() == 1) {
            LatencySampleHolder holder = holders.get(0);
            System.out.println(holder.tag + ": " + holder.getCount());
        } else {
            for (LatencySampleHolder lh: holders) {
               long count = lh.getCount();
               System.out.println(lh.tag + ": " + count);
            }
        }
    }

    public void printRemainingStats() {
        for (LatencySampleHolder lh: holders) {
            lh.printRemainingStats();
        }
    }

    class LatencyCountTransformer<V1> implements ValueTransformer<V1, V1> {
        ProcessorContext ctx;
        LatencySampleHolder lh;

        public LatencyCountTransformer(LatencySampleHolder lh) {
            this.lh = lh;
        }

        @Override
        public void init(ProcessorContext context) {
            ctx = context;
        }

        @Override
        public V1 transform(V1 value) {
            // TODO Auto-generated method stub
            long ts = ctx.timestamp();
            long lat = System.currentTimeMillis() - ts;
            this.lh.AppendLatency(lat);
            return value;
        }

        @Override
        public void close() {
        }
        
    }
    
}
