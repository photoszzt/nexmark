package com.github.nexmark.kafka.model;

import java.time.Instant;

public class BidAndMax {
    public long price;
    public long auction;
    public long bidder;
    public Instant dateTime;
    public String extra;
    public Instant maxDateTime;

    public BidAndMax(long auction, long price,
                     long bidder, Instant dateTime,
                     String extra, Instant maxDateTime) {
        this.auction = auction;
        this.price = price;
        this.bidder = bidder;
        this.dateTime = dateTime;
        this.extra = extra;
        this.maxDateTime = maxDateTime;
    }
}
