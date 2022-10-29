package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AucIdCategory {
    public long id;

    public long category;

    @JsonCreator
    public AucIdCategory(
            @JsonProperty("id") final long id,
            @JsonProperty("category") final long category) {
        this.id = id;
        this.category = category;
    }

    @Override
    public String toString() {
        return "AucIdCat: {id: " + id + ", cat: " + category + "}";
    }
}
