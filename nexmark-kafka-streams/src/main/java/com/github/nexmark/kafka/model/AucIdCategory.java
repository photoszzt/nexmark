package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AucIdCategory {
    @JsonProperty("id")
    public long id;
    @JsonProperty("category")
    public long category;

    @JsonCreator
    public AucIdCategory(long id, long category) {
        this.id = id;
        this.category = category;
    }
}
