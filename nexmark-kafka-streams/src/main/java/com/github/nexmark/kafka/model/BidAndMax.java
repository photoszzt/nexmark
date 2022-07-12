package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BidAndMax {
    public long price;
    public long auction;
    public long bidder;
    public long dateTimeMs;
    public long wStartMs;
    public long wEndMs;

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
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "BidAndMax: {auction: " + auction +
                ", price: " + price +
                ", bidder: " + bidder +
                ", dateTimeMs: " + dateTimeMs +
                ", wStartMs: " + wStartMs +
                ", wEndMs: " + wEndMs + "}";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(auction, price, bidder, dateTimeMs, wStartMs, wEndMs);
    }
}
