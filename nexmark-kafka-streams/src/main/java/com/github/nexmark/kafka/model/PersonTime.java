package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.StartProcTs;

public class PersonTime implements StartProcTs {
    public long id;
    public String name;
    public long startTime;

    @JsonIgnore
    public long startExecNano;

    @JsonCreator
    public PersonTime(@JsonProperty("id") final long id,
                      @JsonProperty("name") final String name,
                      @JsonProperty("startTime") final long startTime) {
        this.id = id;
        this.name = name;
        this.startTime = startTime;
        this.startExecNano = 0;
    }

    @Override
    public String toString() {
        return "PersonTime: {name: " + name +
            ", id: " + id +
            ", startTime: " + startTime + "}";
    }

    @Override
    public boolean equals(final Object o) {
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

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(final long ts) {
        startExecNano = ts;
    }
}
