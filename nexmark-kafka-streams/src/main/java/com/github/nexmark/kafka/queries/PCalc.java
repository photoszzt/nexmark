package com.github.nexmark.kafka.queries;

import java.util.List;

public final class PCalc {
    public static long p(List<Long> latencies,double percent) {
        return latencies.get(((int) ((double)(latencies.size()) * percent + 0.5)) - 1);
    }
}
