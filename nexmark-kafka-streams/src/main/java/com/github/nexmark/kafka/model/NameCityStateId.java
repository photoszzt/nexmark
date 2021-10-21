package com.github.nexmark.kafka.model;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Objects;

import java.io.Serializable;
import javax.annotation.Nullable;

public class NameCityStateId implements Serializable {
    public String name;
    public String city;
    public String state;
    public long id;

    @JsonCreator
    public NameCityStateId(String name, String city, String state, long id) {
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

        NameCityStateId other = (NameCityStateId) otherObject;
        return Objects.equal(name, other.name)
                && Objects.equal(city, other.city)
                && Objects.equal(state, other.state)
                && Objects.equal(id, other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, city, state, id);
    }
}
