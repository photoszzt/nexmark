package com.github.nexmark.kafka.model;

import com.github.nexmark.kafka.queries.StartProcTs;

public class DoubleAndTime implements StartProcTs {
    public double avg;
    public long startExecNano;

    public DoubleAndTime(final double avg) {
        this.avg = avg;
        startExecNano = 0;
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(final long ts) {
        startExecNano = ts;
    }
}
