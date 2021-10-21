package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public class PersonIdName {
    public long id;
    public String name;

    @JsonCreator
    public PersonIdName(long id, String name) {
        this.id = id;
        this.name = name;
    }
}
