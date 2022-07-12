package com.github.nexmark.kafka.model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.io.Serializable;
import javax.annotation.Nullable;

public class NameCityStateId implements Serializable {
    public String name;
    public String city;
    public String state;
    public long id;

    @JsonCreator
    public NameCityStateId(@JsonProperty("name") String name,
                           @JsonProperty("city") String city,
                           @JsonProperty("state") String state,
                           @JsonProperty("id") long id) {
        this.name = name;
        this.city = city;
        this.state = state;
        this.id = id;
    }

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        final NameCityStateId other = (NameCityStateId) otherObject;
        return name.equals(other.name)
                && city.equals(other.city)
                && state.equals(other.state)
                && id == other.id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, city, state, id);
    }
}
