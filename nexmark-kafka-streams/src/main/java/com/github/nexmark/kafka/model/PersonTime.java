package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PersonTime {
    public long id;
    public String name;
    public long startTime;

    @JsonCreator
    public PersonTime(@JsonProperty("id") long id,
                      @JsonProperty("name") String name,
                      @JsonProperty("startTime") long startTime) {
        this.id = id;
        this.name = name;
        this.startTime = startTime;
    }
}
