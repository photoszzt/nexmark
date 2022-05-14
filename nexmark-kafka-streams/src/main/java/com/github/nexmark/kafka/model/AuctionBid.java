package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionBid {
    public long bidDateTimeMs;
    public long aucDateTimeMs;
    public long aucExpiresMs;
    public long bidPrice;
    public long aucCategory;

    @JsonCreator
    public AuctionBid(
            @JsonProperty("bidDateTime") long bidDateTime,
            @JsonProperty("aucDateTime") long aucDateTime,
            @JsonProperty("aucExpires") long aucExpires,
            @JsonProperty("bidPrice") long bidPrice,
            @JsonProperty("aucCategory") long aucCategory) {
        this.bidDateTimeMs = bidDateTime;
        this.aucDateTimeMs = aucDateTime;
        this.aucExpiresMs = aucExpires;
        this.bidPrice = bidPrice;
        this.aucCategory = aucCategory;
    }
}
