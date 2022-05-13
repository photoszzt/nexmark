package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.github.nexmark.kafka.queries.TimestampFromValue;

import javax.annotation.Nullable;
import java.io.Serializable;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Event implements Serializable, TimestampFromValue<Event> {
	@JsonProperty("newPerson")
    public @Nullable Person newPerson;
	@JsonProperty("newAuction")
	public @Nullable Auction newAuction;
	@JsonProperty("bid")
	public @Nullable Bid bid;
	@JsonProperty("etype")
	public EType etype;

	/** The type of object stored in this event. * */
	public enum EType {
		PERSON((byte)0),
		AUCTION((byte)1),
		BID((byte)2);

		public final byte value;

		EType(byte value) {
			this.value = value;
		}

		@JsonValue
		public byte getValue() {
			return value;
		}
	}

	@JsonCreator
	public Event(Person newPerson) {
		this.newPerson = newPerson;
		newAuction = null;
		bid = null;
		etype = EType.PERSON;
	}

	@JsonCreator
	public Event(Auction newAuction) {
		newPerson = null;
		this.newAuction = newAuction;
		bid = null;
		etype = EType.AUCTION;
	}

	@JsonCreator
	public Event(Bid bid) {
		newPerson = null;
		newAuction = null;
		this.bid = bid;
		etype = EType.BID;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Event event = (Event) o;
		return Objects.equals(newPerson, event.newPerson)
			&& Objects.equals(newAuction, event.newAuction)
			&& Objects.equals(bid, event.bid);
	}

	@Override
	public int hashCode() {
		return Objects.hash(newPerson, newAuction, bid);
	}

	@Override
	public String toString() {
		if (newPerson != null) {
			return newPerson.toString();
		} else if (newAuction != null) {
			return newAuction.toString();
		} else if (bid != null) {
			return bid.toString();
		} else {
			throw new RuntimeException("invalid event");
		}
	}

	@Override
	public long extract() {
		if (etype == Event.EType.AUCTION) {
			return newAuction.dateTime.toEpochMilli();
		} else if (etype == Event.EType.PERSON) {
			return newPerson.dateTime.toEpochMilli();
		} else if (etype == Event.EType.BID) {
			return bid.dateTime.toEpochMilli();
		} else {
			throw new IllegalArgumentException("event type should be 0, 1 or 2; got " + etype);
		}
	}
}
