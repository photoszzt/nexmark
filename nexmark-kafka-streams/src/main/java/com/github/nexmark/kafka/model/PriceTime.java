package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.time.Instant;

public class PriceTime {
    public long price;
    public Instant dateTime;

    @JsonCreator
    public PriceTime(long price, Instant dateTime) {
        this.price = price;
        this.dateTime = dateTime;
    }
}
