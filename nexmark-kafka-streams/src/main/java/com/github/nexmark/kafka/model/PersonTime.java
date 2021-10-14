package com.github.nexmark.kafka.model;

import java.time.Instant;

public class PersonTime {
    public long id;
    public String name;
    public long startTime;

    public PersonTime(long id, String name, long startTime) {
        this.id = id;
        this.name = name;
        this.startTime = startTime;
    }
}