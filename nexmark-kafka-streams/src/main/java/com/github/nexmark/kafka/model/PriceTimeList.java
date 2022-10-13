package com.github.nexmark.kafka.model;

import java.io.Serializable;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.nexmark.kafka.queries.StartProcTs;

public class PriceTimeList implements StartProcTs, Serializable {
    public ArrayList<PriceTime> ptlist;

    @JsonIgnore
    private long startProcTsNano;

    @JsonCreator
    public PriceTimeList(@JsonProperty("ptlist") ArrayList<PriceTime> ptlist) {
        this.ptlist = ptlist;
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
