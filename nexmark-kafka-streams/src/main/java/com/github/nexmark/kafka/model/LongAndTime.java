package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;

public class LongAndTime implements StartProcTs, InjTsMs {
    @JsonProperty("val")
    public Long val;

    @JsonIgnore
    public long startExecNano;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public LongAndTime(@JsonProperty("val") Long val) {
        this.val = val;
    }

    @JsonCreator
    public LongAndTime(@JsonProperty("val") long val) {
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

    @Override
    public long injTsMs() {
        return injTsMs;
    }

    @Override
    public void setInjTsMs(long ts) {
        injTsMs = ts; 
    }
}
