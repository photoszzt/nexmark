package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.StartProcTs;
import com.google.common.base.Objects;

import java.io.Serializable;
import javax.annotation.Nullable;

public class NameCityStateId implements Serializable, StartProcTs {
    public String name;
    public String city;
    public String state;
    public long id;

    @JsonIgnore
    public long startExecNano;

    @JsonCreator
    public NameCityStateId(@JsonProperty("name") final String name,
                           @JsonProperty("city") final String city,
                           @JsonProperty("state") final String state,
                           @JsonProperty("id") final long id) {
        this.name = name;
        this.city = city;
        this.state = state;
        this.id = id;
        startExecNano = 0;
    }

    @Override
    public boolean equals(@Nullable final Object otherObject) {
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

    @Override
    public long startProcTsNano() {
        return startExecNano;
    }

    @Override
    public void setStartProcTsNano(final long ts) {
        startExecNano = ts;
    }
}
