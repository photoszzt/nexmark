package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.StartProcTs;

public class BidAndMax implements StartProcTs {
    public long price;
    public long auction;
    public long bidder;
    public long dateTimeMs;
    public long wStartMs;
    public long wEndMs;

    @JsonIgnore
    private long startExecNano;

    @JsonCreator
    public BidAndMax(@JsonProperty("auction") final long auction,
                     @JsonProperty("price") final long price,
                     @JsonProperty("bidder") final long bidder,
                     @JsonProperty("dateTime") final long dateTime,
                     @JsonProperty("wStartMs") final long wStartMs,
                     @JsonProperty("wEndMs") final long wEndMs) {
        this.auction = auction;
        this.price = price;
        this.bidder = bidder;
        this.dateTimeMs = dateTime;
        this.wStartMs = wStartMs;
        this.wEndMs = wEndMs;
        this.startExecNano = 0;
    }

    @Override
    public String toString() {
        return "BidAndMax: {auction: " + auction +
            ", price: " + price +
            ", bidder: " + bidder +
            ", dateTimeMs: " + dateTimeMs +
            ", wStartMs: " + wStartMs +
            ", wEndMs: " + wEndMs + "}";
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(final long ts) {
        startExecNano = ts;
    }
}
