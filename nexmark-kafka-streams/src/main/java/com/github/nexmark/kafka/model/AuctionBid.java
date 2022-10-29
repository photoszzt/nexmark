package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;

public class AuctionBid implements StartProcTs, InjTsMs {
    public long bidDateTimeMs;
    public long aucDateTimeMs;
    public long aucExpiresMs;
    public long bidPrice;
    public long aucCategory;
    public long seller;

    @JsonIgnore
    private long startExecNano;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public AuctionBid(
        @JsonProperty("bidDateTime") final long bidDateTime,
        @JsonProperty("aucDateTime") final long aucDateTime,
        @JsonProperty("aucExpires") final long aucExpires,
        @JsonProperty("bidPrice") final long bidPrice,
        @JsonProperty("aucCategory") final long aucCategory,
        @JsonProperty("seller") final long seller,
        @JsonProperty("injTsMs") final long injTsMs) {
        this.bidDateTimeMs = bidDateTime;
        this.aucDateTimeMs = aucDateTime;
        this.aucExpiresMs = aucExpires;
        this.bidPrice = bidPrice;
        this.aucCategory = aucCategory;
        this.seller = seller;
        this.injTsMs = injTsMs;
        this.startExecNano = 0;
    }

    @Override
    public String toString() {
        return "AuctionBid{" +
            "bidDateTimeMs=" + bidDateTimeMs +
            ", aucDateTimeMs=" + aucDateTimeMs +
            ", aucExpiresMs=" + aucExpiresMs +
            ", bidPrice=" + bidPrice +
            ", aucCategory=" + aucCategory +
            ", seller=" + seller +
            '}';
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(final long ts) {
        startExecNano = ts;
    }

    @Override
    public long injTsMs() {
        return injTsMs;
    }

    @Override
    public void setInjTsMs(final long ts) {
        injTsMs = ts;
    }
}
