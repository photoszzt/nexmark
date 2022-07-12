package com.github.nexmark.kafka.model;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "PersonTime: {name: " + name +
                ", id: " + id +
                ", startTime: " + startTime + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PersonTime other = (PersonTime) o;
        return this.id == other.id && this.startTime == other.startTime &&
                this.name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, startTime);
    }
}
