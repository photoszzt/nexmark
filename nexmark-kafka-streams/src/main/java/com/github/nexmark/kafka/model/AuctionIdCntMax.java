package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.StartProcTs;

public class AuctionIdCntMax implements StartProcTs {
    @JsonProperty("aucId")
    public long aucId;

    @JsonProperty("count")
    public long count;

    @JsonProperty("maxCnt")
    public long maxCnt;

    @JsonIgnore
    public long startExecNano;

    @JsonCreator
    public AuctionIdCntMax(final long aucId,
                           final long count,
                           final long maxCnt) {
        this.aucId = aucId;
        this.count = count;
        this.maxCnt = maxCnt;
        this.startExecNano = 0;
    }

    @Override
    public String toString() {
        return "AuctionIdCntMax: {aucId: " + aucId +
            ", count: " + count +
            ", maxCnt: " + maxCnt + "}";
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
