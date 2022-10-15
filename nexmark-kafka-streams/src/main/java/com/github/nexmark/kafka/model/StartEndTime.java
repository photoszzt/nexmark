package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;

public class StartEndTime implements InjTsMs, StartProcTs {
    public long startTime;
    public long endTime;

    @JsonProperty("injTsMs")
    public long injTsMs;

    @JsonIgnore
    private long startProcTsNano;

    @JsonCreator
    public StartEndTime(@JsonProperty("startTime") long startTime,
                        @JsonProperty("endTime") long endTime,
                        @JsonProperty("injTsMs") long injTsMs) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.injTsMs = injTsMs;
        startProcTsNano = 0;
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

    @Override
    public long injTsMs() {
        return injTsMs;
    }

    @Override
    public void setInjTsMs(long ts) {
        this.injTsMs = ts; 
    }

    @Override
    public long startProcTsNano() {
        return startProcTsNano;
    }

    @Override
    public void setStartProcTsNano(long ts) {
       startProcTsNano = ts; 
    }
}
