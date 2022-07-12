package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuctionIdCntMax {
    public long aucId;
    public long count;
    public long maxCnt;

    @JsonCreator
    public AuctionIdCntMax(@JsonProperty("aucId") long aucId,
                           @JsonProperty("count") long count,
                           @JsonProperty("maxCnt") long maxCnt) {
        this.aucId = aucId;
        this.count = count;
        this.maxCnt = maxCnt;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "AuctionIdCntMax: {aucId: " + aucId +
                ", count: " + count +
                ", maxCnt: " + maxCnt + "}";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(aucId, count, maxCnt);
    }
}
