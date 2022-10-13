package com.github.nexmark.kafka.model;

import java.util.Objects;

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
            @JsonProperty("bidDateTime") long bidDateTime,
            @JsonProperty("aucDateTime") long aucDateTime,
            @JsonProperty("aucExpires") long aucExpires,
            @JsonProperty("bidPrice") long bidPrice,
            @JsonProperty("aucCategory") long aucCategory,
            @JsonProperty("seller") long seller) {
        this.bidDateTimeMs = bidDateTime;
        this.aucDateTimeMs = aucDateTime;
        this.aucExpiresMs = aucExpires;
        this.bidPrice = bidPrice;
        this.aucCategory = aucCategory;
        this.seller = seller;
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
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(bidDateTimeMs, aucDateTimeMs, aucExpiresMs, bidPrice, aucCategory, seller);
    }

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(long ts) {
       startExecNano = ts; 
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
