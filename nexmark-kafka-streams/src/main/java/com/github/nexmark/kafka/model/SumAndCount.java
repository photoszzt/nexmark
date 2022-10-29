package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.StartProcTs;

public class SumAndCount implements StartProcTs {
    public long sum;
    public long count;
    @JsonProperty("startExecNano")
    public long startExecNano;

    @JsonCreator
    public SumAndCount(@JsonProperty("sum") final long sum,
                       @JsonProperty("count") final long count,
                       @JsonProperty("startExecNano") final long startExecNano) {
        this.sum = sum;
        this.count = count;
        this.startExecNano = startExecNano;
    }

    @Override
    public String toString() {
        return "SumAndCount: {Sum: " + sum + ", Count: " + count + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SumAndCount other = (SumAndCount) o;
        return this.sum == other.sum && this.count == other.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count);
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
