package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;

public class AuctionIdCount implements StartProcTs, InjTsMs {
    public long aucId;
    public long count;

    @JsonIgnore
    public long startExecNano;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public AuctionIdCount(@JsonProperty("aucId") final long aucId,
                          @JsonProperty("count") final long count,
                          @JsonProperty("injTsMs") final long injTsMs) {
        this.aucId = aucId;
        this.count = count;
        this.injTsMs = injTsMs;
        this.startExecNano = 0;
    }

    @Override
    public String toString() {
        return "AuctionIdCount: {aucId: " + aucId +
            ", count: " + count + "}";
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
