package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public class AuctionIdCount {
    public long aucId;
    public long count;

    @JsonCreator
    public AuctionIdCount(long aucId, long count) {
        this.aucId = aucId;
        this.count = count;
    }
}
