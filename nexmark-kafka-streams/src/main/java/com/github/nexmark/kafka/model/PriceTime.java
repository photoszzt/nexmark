package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PriceTime {
    public long price;
    public long dateTime;

    @JsonCreator
    public PriceTime(@JsonProperty("price") long price,
                     @JsonProperty("dateTime") long dateTime) {
        this.price = price;
        this.dateTime = dateTime;
    }
}
