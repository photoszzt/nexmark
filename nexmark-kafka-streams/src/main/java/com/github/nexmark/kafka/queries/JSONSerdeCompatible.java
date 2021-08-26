package com.github.nexmark.kafka.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.nexmark.kafka.model.Auction;
import com.github.nexmark.kafka.model.Bid;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.Person;
import sun.plugin.util.UserProfile;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Auction.class),
        @JsonSubTypes.Type(value = Event.class),
        @JsonSubTypes.Type(value = Person.class),
        @JsonSubTypes.Type(value = Bid.class)
})
public interface JSONSerdeCompatible {
}
