package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StartEndTime {
    public long startTime;
    public long endTime;

    @JsonCreator
    public StartEndTime(@JsonProperty("startTime") long startTime,
                        @JsonProperty("endTime") long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "StartEndTime: {startTime: " + startTime +
                ", endTime: " + endTime + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StartEndTime other = (StartEndTime) o;
        return this.startTime == other.startTime && this.endTime == other.endTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime);
    }
}
