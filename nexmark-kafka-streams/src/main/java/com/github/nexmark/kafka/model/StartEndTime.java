package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public class StartEndTime {
    public long startTime;
    public long endTime;

    @JsonCreator
    public StartEndTime(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
