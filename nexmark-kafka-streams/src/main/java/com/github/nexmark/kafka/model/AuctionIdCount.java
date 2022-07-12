package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuctionIdCount {
    public long aucId;
    public long count;

    @JsonCreator
    public AuctionIdCount(@JsonProperty("aucId") long aucId,
                          @JsonProperty("count") long count) {
        this.aucId = aucId;
        this.count = count;
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
}
