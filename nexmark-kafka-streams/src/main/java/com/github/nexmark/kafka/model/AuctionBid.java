package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.time.Instant;

public class AuctionBid {
    public Instant bidDateTime;
    public Instant aucDateTime;
    public Instant aucExpires;
    public long bidPrice;
    public long aucCategory;

    @JsonCreator
    public AuctionBid(Instant bidDateTime, Instant aucDateTime, 
        Instant aucExpires, long bidPrice, long aucCategory) {
        this.bidDateTime = bidDateTime;
        this.aucDateTime = aucDateTime;
        this.aucExpires = aucExpires;
        this.bidPrice = bidPrice;
        this.aucCategory = aucCategory;
    }
}
