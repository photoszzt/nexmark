package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AucIDSeller {
    @JsonProperty("id")
    public long id;
    @JsonProperty("seller")
    public long seller;

    @JsonCreator
    public AucIDSeller(long id, long seller) {
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
}
