package com.github.nexmark.kafka.model;

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
}
