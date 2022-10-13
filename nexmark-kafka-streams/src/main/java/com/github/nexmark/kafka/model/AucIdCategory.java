package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;

public class AucIdCategory implements InjTsMs{
    public long id;

    public long category;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonCreator
    public AucIdCategory(
            @JsonProperty("id") long id,
            @JsonProperty("category") long category) {
        this.id = id;
        this.category = category;
    }

    @Override
    public String toString() {
        return "AucIdCat: {id: " + id + ", cat: " + category + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, category);
    }

    @Override
    public long injTsMs() {
        return injTsMs;
    }

    @Override
    public void setInjTsMs(long ts) {
        this.injTsMs = ts;
    }
}
