package com.github.nexmark.kafka.model;

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
        // TODO Auto-generated method stub
        return "SumAndCount: {Sum: " + sum + ", Count: " + count + "}"; 
    }
}
