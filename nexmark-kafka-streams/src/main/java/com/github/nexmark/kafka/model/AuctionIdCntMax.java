package com.github.nexmark.kafka.model;

import java.util.Objects;

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
    public AuctionIdCntMax(long aucId,
            long count,
            long maxCnt) {
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
    public int hashCode() {
        return Objects.hash(aucId, count, maxCnt);
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
