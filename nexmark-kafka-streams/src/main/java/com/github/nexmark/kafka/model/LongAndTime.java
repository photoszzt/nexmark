package com.github.nexmark.kafka.model;

import com.github.nexmark.kafka.queries.StartProcTs;

public class LongAndTime implements StartProcTs {
    public Long val;
    public long startExecNano;

    public LongAndTime(Long val) {
        this.val = val;
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
