package com.github.nexmark.kafka.queries;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class LatencyCountTransformerSupplier<V extends StartProcTs, V1> implements ValueTransformerSupplier<V, V1> {

    List<LatencyHolder> holders = new ArrayList<>();
    String tag;
    String baseDir;
    ValueMapper<V, V1> mapper;

    public LatencyCountTransformerSupplier(String tag, String baseDir, ValueMapper<V, V1> mapper) {
        this.tag = tag;
        this.baseDir = baseDir;
        this.mapper = mapper;
    }

    public void SetAfterWarmup(){
        for (LatencyHolder lh : holders) {
            lh.SetAfterWarmup();
        }
    }

    @Override
    public ValueTransformer<V, V1> get() {
        String lhTag = tag + "-" + holders.size();
        String path = baseDir + File.separator + lhTag;
        try {
            holders.add(new LatencyHolder(lhTag, path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new LatencyCountTransformer<V, V1>(holders.get(holders.size() - 1), mapper);
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

    class LatencyCountTransformer<VV extends StartProcTs, VV1> implements ValueTransformer<VV, VV1> {
        ProcessorContext ctx;
        LatencyHolder lh;
        ArrayList<Long> procLat = new ArrayList<>(NUM_STATS);
        ValueMapper<VV, VV1> mapper;

        public LatencyCountTransformer(LatencyHolder lh, ValueMapper<VV, VV1> mapper) {
            this.lh = lh;
            this.mapper = mapper;
        }

        @Override
        public void init(ProcessorContext context) {
            ctx = context;
            lh.init();
        }

        @Override
        public VV1 transform(VV value) {
            long ts = ctx.timestamp();
            long now = Instant.now().toEpochMilli();
            long startProc = value.startProcTsNano();
            // assert (startProc != 0);
            long lat = System.nanoTime() - startProc;
            StreamsUtils.appendLat(procLat, lat, lh.tag + "_proc");
            lh.appendLatency(now - ts);
            return mapper.apply(value);
        }

        @Override
        public void close() {
            lh.waitForFinish();
            if (procLat.size() > 0) {
                System.out.println("{\"" + lh.tag + "_proc" + "\": " + procLat + "}");
            }
        }
    }
}
