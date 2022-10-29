package com.github.nexmark.kafka.queries;

import java.util.List;

public final class PCalc {
    public static long p(final List<Long> latencies, final double percent) {
        return latencies.get(((int) ((double) (latencies.size()) * percent + 0.5)) - 1);
    }
}
