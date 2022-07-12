package com.github.nexmark.kafka.model;

import java.util.Comparator;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PriceTime other = (PriceTime) o;
        return this.price == other.price && this.ts == other.ts;
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, ts);
    }

    public static final Comparator<PriceTime> ASCENDING_TIME_THEN_PRICE =
      Comparator.comparing((PriceTime pt) -> pt.ts).thenComparingLong(pt -> pt.price);
}
