package com.github.nexmark.kafka.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LatencyCountTransformerSupplier<V> implements ValueTransformerSupplier<V, V>{

    List<LatencyHolder> holders = new ArrayList<>();
    String tag;
    int numHolders;

    public LatencyCountTransformerSupplier(String tag) {
        this.tag = tag;
    }

    @Override
    public ValueTransformer<V, V> get() {
        // TODO Auto-generated method stub
        LatencyHolder holder = new LatencyHolder(tag+"-"+numHolders);
        holders.add(holder);
        numHolders += 1;
        return new LatencyCountTransformer<V>(holder);
    }

    public void SetAfterWarmup(){
        for (LatencyHolder lh : holders) {
            lh.SetAfterWarmup();
        }
    }

    public void printCount() {
        if (holders.size() == 1) {
            LatencyHolder holder = holders.get(0);
            System.out.println(holder.tag + ": " + holder.getCount());
        } else {
            for (LatencyHolder lh: holders) {
               long count = lh.getCount();
               System.out.println(lh.tag + ": " + count);
            }
        }
    }

    class LatencyCountTransformer<V1> implements ValueTransformer<V1, V1> {
        ProcessorContext ctx;
        LatencyHolder lh;

        public LatencyCountTransformer(LatencyHolder lh) {
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
