package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public class SumAndCount {
    public long sum;
    public long count;

    @JsonCreator
    public SumAndCount(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }
}
