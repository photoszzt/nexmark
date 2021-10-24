package com.github.nexmark.kafka.model;

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
}
