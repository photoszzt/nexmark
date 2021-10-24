package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class BidAndMax {
    public long price;
    public long auction;
    public long bidder;
    public Instant dateTime;
    public String extra;
    public Instant maxDateTime;

    @JsonCreator
    public BidAndMax(@JsonProperty("auction") long auction,
                     @JsonProperty("price") long price,
                     @JsonProperty("bidder") long bidder,
                     @JsonProperty("dateTime") Instant dateTime,
                     @JsonProperty("extra") String extra,
                     @JsonProperty("maxDateTime") Instant maxDateTime) {
        this.auction = auction;
        this.price = price;
        this.bidder = bidder;
        this.dateTime = dateTime;
        this.extra = extra;
        this.maxDateTime = maxDateTime;
    }
}
