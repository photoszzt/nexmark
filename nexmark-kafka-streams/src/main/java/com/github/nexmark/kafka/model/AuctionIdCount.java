package com.github.nexmark.kafka.model;

import java.util.Objects;

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

    @JsonIgnore
    public long injTsMs;

    @JsonCreator
    public AuctionIdCount(@JsonProperty("aucId") long aucId,
            @JsonProperty("count") long count,
            @JsonProperty("injTsMs") long injTsMs) {
        this.aucId = aucId;
        this.count = count;
        this.injTsMs = injTsMs;
        this.startExecNano = 0;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "AuctionIdCount: {aucId: " + aucId +
                ", count: " + count + "}";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(aucId, count);
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
