package com.github.nexmark.kafka.queries;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LatencyCountTransformerSupplier<V> implements ValueTransformerSupplier<V, V>{

    List<LatencyHolder> holders = new ArrayList<>();

    @Override
    public ValueTransformer<V, V> get() {
        // TODO Auto-generated method stub
        LatencyHolder holder = new LatencyHolder();
        holders.add(holder);
        return new LatencyCountTransformer<V>(holder);
    }

    public void SetAfterWarmup(){
        for (LatencyHolder lh : holders) {
            lh.SetAfterWarmup();
        }
    }

    public List<Long> GetLatency() {
        if (holders.size() == 1) {
            return holders.get(0).GetLatency();
        } else {
            List<Long> lats = new ArrayList<>();
            for (LatencyHolder lh: holders) {
                lats.addAll(lh.GetLatency());
            }
            return lats;
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
