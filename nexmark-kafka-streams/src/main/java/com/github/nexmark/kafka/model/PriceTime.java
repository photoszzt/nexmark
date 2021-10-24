package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class PriceTime {
    public long price;
    public Instant dateTime;

    @JsonCreator
    public PriceTime(@JsonProperty("price") long price,
                     @JsonProperty("dateTime") Instant dateTime) {
        this.price = price;
        this.dateTime = dateTime;
    }
}
