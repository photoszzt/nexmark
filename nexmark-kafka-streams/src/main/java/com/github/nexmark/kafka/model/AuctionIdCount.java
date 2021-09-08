package com.github.nexmark.kafka.model;

public class AuctionIdCount {
    public long aucId;
    public long count;

    public AuctionIdCount(long aucId, long count) {
        this.aucId = aucId;
        this.count = count;
    }
}
