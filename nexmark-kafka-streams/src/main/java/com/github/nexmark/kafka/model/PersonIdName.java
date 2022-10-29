package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PersonIdName {
    public long id;
    public String name;

    @JsonCreator
    public PersonIdName(@JsonProperty("id") final long id,
                        @JsonProperty("name") final String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "PersonIdName: {id: " + id +
            ", name: " + name + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PersonIdName other = (PersonIdName) o;
        return this.id == other.id && this.name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
