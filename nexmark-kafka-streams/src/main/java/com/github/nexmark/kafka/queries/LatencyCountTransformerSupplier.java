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

    private final List<LatencyHolder> holders = new ArrayList<>();
    private final String tag;
    private final String baseDir;
    private final ValueMapper<V, V1> mapper;

    public LatencyCountTransformerSupplier(final String tag, final String baseDir,
                                           final ValueMapper<V, V1> mapper) {
        this.tag = tag;
        this.baseDir = baseDir;
        this.mapper = mapper;
    }

    public void SetAfterWarmup() {
        for (final LatencyHolder lh : holders) {
            lh.SetAfterWarmup();
        }
    }

    @Override
    public ValueTransformer<V, V1> get() {
        final String lhTag = tag + "-" + holders.size();
        final String path = baseDir + File.separator + lhTag;
        try {
            holders.add(new LatencyHolder(lhTag, path));
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return new LatencyCountTransformer<>(holders.get(holders.size() - 1), mapper);
    }

    public void printCount() {
        if (holders.size() == 1) {
            final LatencyHolder holder = holders.get(0);
            System.out.println(holder.tag() + ": " + holder.getCount());
        } else {
            for (final LatencyHolder lh : holders) {
                final long count = lh.getCount();
                System.out.println(lh.tag() + ": " + count);
            }
        }
    }

    public void waitForFinish() {
        for (final LatencyHolder lh : holders) {
            lh.waitForFinish();
        }
    }

    public void outputRemainingStats() {
        for (final LatencyHolder lh : holders) {
            lh.outputRemainingStats();
        }
    }

    static class LatencyCountTransformer<VV extends StartProcTs, VV1> implements ValueTransformer<VV, VV1> {
        ProcessorContext ctx;
        LatencyHolder lh;
        ArrayList<Long> procLat = new ArrayList<>(NUM_STATS);
        ValueMapper<VV, VV1> mapper;

        public LatencyCountTransformer(final LatencyHolder lh, final ValueMapper<VV, VV1> mapper) {
            this.lh = lh;
            this.mapper = mapper;
        }

        @Override
        public void init(final ProcessorContext context) {
            ctx = context;
            lh.init();
        }

        @Override
        public VV1 transform(final VV value) {
            final long ts = ctx.timestamp();
            final long now = Instant.now().toEpochMilli();
            final long startProc = value.startProcTsNano();
            assert startProc != 0;
            final long lat = System.nanoTime() - startProc;
            StreamsUtils.appendLat(procLat, lat, lh.tag() + "_proc");
            lh.appendLatency(now - ts);
            return mapper.apply(value);
        }

        @Override
        public void close() {
            lh.waitForFinish();
            if (procLat.size() > 0) {
                System.out.println("{\"" + lh.tag() + "_proc" + "\": " + procLat + "}");
            }
        }
    }
}
