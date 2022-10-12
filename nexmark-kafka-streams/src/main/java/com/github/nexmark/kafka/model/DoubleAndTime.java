package com.github.nexmark.kafka.model;

import com.github.nexmark.kafka.queries.StartProcTs;

public class DoubleAndTime implements StartProcTs{
    public double avg;
    public long startExecNano;

    public DoubleAndTime(double avg) {
        this.avg = avg;
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(long ts) {
        startExecNano = ts; 
    }
}
