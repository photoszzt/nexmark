package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;

public class AucIDSeller implements InjTsMs{
    public long id;
    public long seller;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public AucIDSeller(
            @JsonProperty("id") long id,
            @JsonProperty("seller") long seller) {
        this.id = id;
        this.seller = seller;
    }

    @Override
    public String toString() {
        return "AucIDSeller: {id: " + id + ", seller: " + seller + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, seller);
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
