package com.github.nexmark.kafka.model;

import java.time.Instant;

public class PriceTime {
    public long price;
    public Instant dateTime;

    public PriceTime(long price, Instant dateTime) {
        this.price = price;
        this.dateTime = dateTime;
    }
}
