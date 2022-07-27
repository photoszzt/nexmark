package com.github.nexmark.kafka.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;

public class LatencyHolder {
    List<Long> latencies;
    long counter;
    AtomicBoolean afterWarmup;
    private static final int MIN_COLLECT = 200;
    private static final Duration DEFAULT_COLLECT_INTERVAL = Duration.ofSeconds(10);
    ReportTimer rt;
    String tag;

    public LatencyHolder(String tag) {
        latencies = new ArrayList<>();
        afterWarmup = new AtomicBoolean(false);
        this.tag = tag;
        rt = new ReportTimer(DEFAULT_COLLECT_INTERVAL);
        latencies = new ArrayList<>(MIN_COLLECT);
        counter = 0;
    }

    public void SetAfterWarmup() {
        afterWarmup.set(true);
    }

    public void AppendLatency(long lat) {
        // if (afterWarmup.get()) {
        counter += 1;
        latencies.add(lat);
        if (latencies.size() >= MIN_COLLECT && rt.Check()) {
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

    public long getCount() {
        return counter;
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
}
