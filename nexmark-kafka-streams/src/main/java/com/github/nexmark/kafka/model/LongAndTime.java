package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;

public class LongAndTime implements StartProcTs, InjTsMs {
    @JsonProperty("val")
    public Long val;

    @JsonProperty("startExecNano")
    public long startExecNano;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public LongAndTime(@JsonProperty("val") final Long val,
                       @JsonProperty("injTsMs") final long injTsMs,
                       @JsonProperty("startExecNano") final long startExecNano) {
        this.val = val;
        this.injTsMs = injTsMs;
        this.startExecNano = startExecNano;
    }

    @JsonCreator
    public LongAndTime(@JsonProperty("val") final long val,
                       @JsonProperty("injTsMs") final long injTsMs,
                       @JsonProperty("startExecNano") final long startExecNano) {
        this.val = val;
        this.injTsMs = injTsMs;
        this.startExecNano = startExecNano;
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(final long ts) {
        startExecNano = ts;
    }

    @Override
    public long injTsMs() {
        return injTsMs;
    }

    @Override
    public void setInjTsMs(final long ts) {
        injTsMs = ts;
    }
}
