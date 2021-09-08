package com.github.nexmark.kafka.model;

public class AuctionIdCntMax {
    public long aucId;
    public long count;
    public long maxCnt;

    public AuctionIdCntMax(long aucId, long count, long maxCnt) {
        this.aucId = aucId;
        this.count = count;
        this.maxCnt = maxCnt;
    }
}
