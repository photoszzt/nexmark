package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AucIDSeller {
    public long id;
    public long seller;

    @JsonCreator
    public AucIDSeller(
        @JsonProperty("id") final long id,
        @JsonProperty("seller") final long seller) {
        this.id = id;
        this.seller = seller;
    }

    @Override
    public String toString() {
        return "AucIDSeller: {id: " + id + ", seller: " + seller + "}";
    }
}
