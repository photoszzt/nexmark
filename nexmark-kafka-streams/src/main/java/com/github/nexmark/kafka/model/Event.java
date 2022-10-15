package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.github.nexmark.kafka.queries.InjTsMs;
import com.github.nexmark.kafka.queries.StartProcTs;
import com.github.nexmark.kafka.queries.TimestampFromValue;

import javax.annotation.Nullable;
import java.io.Serializable;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Event implements Serializable, TimestampFromValue<Event>, StartProcTs, InjTsMs {
	@JsonProperty("newPerson")
    public @Nullable Person newPerson;
	@JsonProperty("newAuction")
	public @Nullable Auction newAuction;
	@JsonProperty("bid")
	public @Nullable Bid bid;
	@JsonProperty("etype")
	public EType etype;
	@JsonProperty("injTsMs")
	public long injTsMs;

	@JsonIgnore
	private long startExecNano;

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
	public Event(Person newPerson, long injTsMs) {
		this.newPerson = newPerson;
		newAuction = null;
		bid = null;
		etype = EType.PERSON;
		startExecNano = 0;
		this.injTsMs = injTsMs;
	}

	@JsonCreator
	public Event(Auction newAuction, long injTsMs) {
		newPerson = null;
		this.newAuction = newAuction;
		bid = null;
		etype = EType.AUCTION;
		startExecNano = 0;
		this.injTsMs = injTsMs;
	}

	@JsonCreator
	public Event(Bid bid, long injTsMs) {
		newPerson = null;
		newAuction = null;
		this.bid = bid;
		etype = EType.BID;
		startExecNano = 0;
		this.injTsMs = injTsMs;
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
			return newAuction.dateTime;
		} else if (etype == Event.EType.PERSON) {
			return newPerson.dateTime;
		} else if (etype == Event.EType.BID) {
			return bid.dateTime;
		} else {
			throw new IllegalArgumentException("event type should be 0, 1 or 2; got " + etype);
		}
	}

	@Override
	public long startProcTsNano() {
		return this.startExecNano;
	}

	@Override
	public void setStartProcTsNano(long ts) {
		this.startExecNano = ts;	
	}

	@Override
	public long injTsMs() {
		return injTsMs;
	}

	@Override
	public void setInjTsMs(long ts) {
		this.injTsMs = ts;
	}
}
