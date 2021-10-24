package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PersonIdName {
    public long id;
    public String name;

    @JsonCreator
    public PersonIdName(@JsonProperty("id") long id,
                        @JsonProperty("name") String name) {
        this.id = id;
        this.name = name;
    }
}
