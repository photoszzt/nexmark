package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionBid {
    public Instant bidDateTime;
    public Instant aucDateTime;
    public Instant aucExpires;
    public long bidPrice;
    public long aucCategory;

    @JsonCreator
    public AuctionBid(
            @JsonProperty("bidDateTime") Instant bidDateTime,
            @JsonProperty("aucDateTime") Instant aucDateTime,
            @JsonProperty("aucExpires") Instant aucExpires,
            @JsonProperty("bidPrice") long bidPrice,
            @JsonProperty("aucCategory") long aucCategory) {
        this.bidDateTime = bidDateTime;
        this.aucDateTime = aucDateTime;
        this.aucExpires = aucExpires;
        this.bidPrice = bidPrice;
        this.aucCategory = aucCategory;
    }
}
