package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public class AuctionIdCntMax {
    public long aucId;
    public long count;
    public long maxCnt;

    @JsonCreator
    public AuctionIdCntMax(long aucId, long count, long maxCnt) {
        this.aucId = aucId;
        this.count = count;
        this.maxCnt = maxCnt;
    }
}
