package com.github.nexmark.kafka.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;

import org.apache.kafka.streams.kstream.ForeachAction;

public class LatencyCountSample<K, V extends TimestampFromValue<V>> implements ForeachAction<K, TimestampFromValue<V>> {
    List<Long> latencies;
    long counter;
    AtomicBoolean afterWarmup = new AtomicBoolean(false);
    private static final int MIN_COLLECT = 200;
    private static final Duration DEFAULT_COLLECT_INTERVAL = Duration.ofSeconds(10);
    ReportTimer rt;
    String tag;

    public LatencyCountSample(String tag) {
        rt = new ReportTimer(DEFAULT_COLLECT_INTERVAL);
        this.tag = tag;
        latencies = new ArrayList<>(MIN_COLLECT);
        counter = 0;
    }

    @Override
    public void apply(K key, TimestampFromValue<V> value) {
        // if (afterWarmup.get()) {
        counter += 1;
        long ts = value.extract();
        long lat = System.currentTimeMillis() - ts;
        latencies.add(lat);
        if (rt.Check() && latencies.size() >= MIN_COLLECT) {
            latencies.sort( (a, b) -> (int) (a - b));
            long p50 = PCalc.p(latencies, 0.5);
            long p90 = PCalc.p(latencies, 0.9);
            long p99 = PCalc.p(latencies, 0.99);
            Duration dur = rt.Mark();
            System.out.printf("%s stats (%d samples): dur=%d ms, p50=%d, p90=%d, p99=%d%n",
                    tag, latencies.size(), dur.toMillis(), p50, p90, p99);
            latencies = new ArrayList<>(MIN_COLLECT);
        }
        // }
    }

    public void printCount() {
        System.out.println(tag + ": " + counter);
    }

    public void printRemainingStats() {
        if (latencies.size() > 0) {
            latencies.sort( (a, b) -> (int) (a - b));
            long p50 = PCalc.p(latencies, 0.5);
            long p90 = PCalc.p(latencies, 0.9);
            long p99 = PCalc.p(latencies, 0.99);
            Duration dur = rt.Mark();
            System.out.printf("%s stats (%d samples): dur=%d ms, p50=%d, p90=%d, p99=%d%n",
                    tag, latencies.size(), dur.toMillis(), p50, p90, p99);
        }
    }

    public void SetAfterWarmup() {
        afterWarmup.set(true);
    }
}
