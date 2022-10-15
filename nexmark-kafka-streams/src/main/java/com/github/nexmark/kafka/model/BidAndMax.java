package com.github.nexmark.kafka.model;

import java.util.Objects;

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
    public BidAndMax(@JsonProperty("auction") long auction,
            @JsonProperty("price") long price,
            @JsonProperty("bidder") long bidder,
            @JsonProperty("dateTime") long dateTime,
            @JsonProperty("wStartMs") long wStartMs,
            @JsonProperty("wEndMs") long wEndMs) {
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
    public int hashCode() {
        return Objects.hash(auction, price, bidder, dateTimeMs, wStartMs, wEndMs);
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(long ts) {
        startExecNano = ts;
    }
}
