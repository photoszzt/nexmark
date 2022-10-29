package com.github.nexmark.kafka.queries;

import java.time.Duration;
import java.time.Instant;

public class ReportTimer {
    Instant lastTs;
    Duration duration;

    public ReportTimer(final Duration duration) {
        this.duration = duration;
        this.lastTs = null;
    }

    public boolean Check() {
        if (lastTs == null) {
            lastTs = Instant.now();
            return false;
        }
        return (Duration.between(lastTs, Instant.now()).compareTo(duration) >= 0);
    } 

    public Duration Mark() {
        Duration dur = Duration.between(lastTs, Instant.now());
        lastTs = Instant.now();
        return dur;
    }
}
