package com.github.nexmark.kafka.queries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LatencyCountTransformerSupplier<V> implements ValueTransformerSupplier<V, V> {

    List<LatencyHolder> holders = new ArrayList<>();
    String tag;
    String baseDir;

    public LatencyCountTransformerSupplier(String tag, String baseDir) {
        this.tag = tag;
        this.baseDir = baseDir;
    }

    public void SetAfterWarmup(){
        for (LatencyHolder lh : holders) {
            lh.SetAfterWarmup();
        }
    }

    @Override
    public ValueTransformer<V, V> get() {
        String lhTag = tag + "-" + holders.size();
        String path = baseDir + File.separator + lhTag;
        try {
            holders.add(new LatencyHolder(lhTag, path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new LatencyCountTransformer<V>(holders.get(holders.size() - 1));
    }

    public void printCount() {
        if (holders.size() == 1) {
            LatencyHolder holder = holders.get(0);
            System.out.println(holder.tag + ": " + holder.getCount());
        } else {
            for (LatencyHolder lh : holders) {
                long count = lh.getCount();
                System.out.println(lh.tag + ": " + count);
            }
        }
    }

    public void waitForFinish() {
        for (LatencyHolder lh : holders) {
            lh.waitForFinish();
        }
    }

    public void outputRemainingStats() {
        for (LatencyHolder lh : holders) {
            lh.outputRemainingStats();
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
            lh.init();
        }

        @Override
        public V1 transform(V1 value) {
            long ts = ctx.timestamp();
            long now = System.currentTimeMillis();
            lh.appendLatency(now - ts);
            return value;
        }

        @Override
        public void close() {
            lh.waitForFinish();
        }
    }
}
