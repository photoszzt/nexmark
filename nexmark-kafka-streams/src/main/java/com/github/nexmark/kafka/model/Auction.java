package com.github.nexmark.kafka.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class Auction implements Serializable {
    /** Id of auction. */
	public long id; // primary key

	/** Extra auction properties. */
	public String itemName;

	public String description;

	/** Initial bid price, in cents. */
	public long initialBid;

	/** Reserve price, in cents. */
	public long reserve;

	@JsonProperty("dateTime")
	public long dateTimeMs;

	/** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
	@JsonProperty("expires")
	public long expiresMs;

	/** Id of person who instigated auction. */
	public long seller; // foreign key: Person.id

	/** Id of category auction is listed under. */
	public long category; // foreign key: Category.id

	/** Additional arbitrary payload for performance testing. */
	public String extra;

	@JsonCreator
	public Auction(
			@JsonProperty("id") long id,
			@JsonProperty("itemName") String itemName,
			@JsonProperty("description") String description,
			@JsonProperty("initialBid") long initialBid,
			@JsonProperty("reserve") long reserve,
			@JsonProperty("dateTime") long dateTime,
			@JsonProperty("expires") long expires,
			@JsonProperty("seller") long seller,
			@JsonProperty("category") long category,
			@JsonProperty("extra") String extra) {
		this.id = id;
		this.itemName = itemName;
		this.description = description;
		this.initialBid = initialBid;
		this.reserve = reserve;
		this.dateTimeMs = dateTime;
		this.expiresMs = expires;
		this.seller = seller;
		this.category = category;
		this.extra = extra;
	}

	@Override
	public String toString() {
		return "Auction{" +
				"id=" + id +
				", itemName='" + itemName + '\'' +
				", description='" + description + '\'' +
				", initialBid=" + initialBid +
				", reserve=" + reserve +
				", dateTime=" + dateTimeMs +
				", expires=" + expiresMs +
				", seller=" + seller +
				", category=" + category +
				", extra='" + extra + '\'' +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Auction auction = (Auction) o;
		return id == auction.id
			&& initialBid == auction.initialBid
			&& reserve == auction.reserve
			&& Objects.equal(dateTimeMs, auction.dateTimeMs)
			&& Objects.equal(expiresMs, auction.expiresMs)
			&& seller == auction.seller
			&& category == auction.category
			&& Objects.equal(itemName, auction.itemName)
			&& Objects.equal(description, auction.description)
			&& Objects.equal(extra, auction.extra);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(
			id, itemName, description, initialBid, reserve, dateTimeMs, expiresMs, seller, category, extra);
	}
    
}
