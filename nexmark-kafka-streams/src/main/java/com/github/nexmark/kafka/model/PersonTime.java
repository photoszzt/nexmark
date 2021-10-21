package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public class PersonTime {
    public long id;
    public String name;
    public long startTime;

    @JsonCreator
    public PersonTime(long id, String name, long startTime) {
        this.id = id;
        this.name = name;
        this.startTime = startTime;
    }
}
