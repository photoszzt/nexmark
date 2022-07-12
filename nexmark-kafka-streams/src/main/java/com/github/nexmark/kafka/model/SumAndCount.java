package com.github.nexmark.kafka.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SumAndCount {
    public long sum;
    public long count;

    @JsonCreator
    public SumAndCount(@JsonProperty("sum") long sum,
                       @JsonProperty("count") long count) {
        this.sum = sum;
        this.count = count;
    }

    @Override
    public String toString() {
        return "SumAndCount: {Sum: " + sum + ", Count: " + count + "}"; 
    }

    @Override
    public boolean equals(Object o) {
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
}
