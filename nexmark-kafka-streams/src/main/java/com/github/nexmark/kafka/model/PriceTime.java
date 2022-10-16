package com.github.nexmark.kafka.model;

import java.util.Comparator;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;

public class PriceTime implements StartProcTs, InjTsMs {
    public long price;
    public long ts;

    @JsonProperty("startExecNano")
    public long startProcTsNano;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public PriceTime(
            @JsonProperty("price") long price,
            @JsonProperty("ts") long ts,
            @JsonProperty("injTsMs") long injTsMs,
            @JsonProperty("startExecNano") long startProcTsNano) {
        this.ts = ts;
        this.price = price;
        this.injTsMs = injTsMs;
        this.startProcTsNano = startProcTsNano;
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

    @Override
    public long startProcTsNano() {
        return startProcTsNano;
    }

    @Override
    public void setStartProcTsNano(long ts) {
        this.startProcTsNano = ts; 
    }

    @Override
    public long injTsMs() {
        return injTsMs;
    }

    @Override
    public void setInjTsMs(long ts) {
        injTsMs = ts; 
    }
}
