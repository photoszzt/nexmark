package com.github.nexmark.kafka.model;

import java.util.Comparator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PriceTime {
    public long price;
    public long ts;

    @JsonCreator
    public PriceTime(
            @JsonProperty("price") long price,
            @JsonProperty("ts") long ts) {
        this.ts = ts;
        this.price = price;
    }

    @Override
    public String toString() {
        return "PriceTime: {price: " + price + ", ts: " + ts + "}";
    } 

    public static final Comparator<PriceTime> ASCENDING_TIME_THEN_PRICE =
      Comparator.comparing((PriceTime pt) -> pt.ts).thenComparingLong(pt -> pt.price);
}
