package com.github.nexmark.kafka.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class Auction implements Serializable {
    /**
     * Id of auction.
     */
    public final long id; // primary key

    /**
     * Extra auction properties.
     */
    public final String itemName;

    public final String description;

    /**
     * Initial bid price, in cents.
     */
    public final long initialBid;

    /**
     * Reserve price, in cents.
     */
    public final long reserve;

    @JsonProperty("dateTime")
    public final long dateTime;

    /**
     * When does auction expire? (ms since epoch). Bids at or after this time are ignored.
     */
    @JsonProperty("expires")
    public final long expires;

    /**
     * Id of person who instigated auction.
     */
    public final long seller; // foreign key: Person.id

    /**
     * Id of category auction is listed under.
     */
    public final long category; // foreign key: Category.id

    /**
     * Additional arbitrary payload for performance testing.
     */
    public final String extra;

    @JsonCreator
    public Auction(
        @JsonProperty("id") final long id,
        @JsonProperty("itemName") final String itemName,
        @JsonProperty("description") final String description,
        @JsonProperty("initialBid") final long initialBid,
        @JsonProperty("reserve") final long reserve,
        @JsonProperty("dateTime") final long dateTime,
        @JsonProperty("expires") final long expires,
        @JsonProperty("seller") final long seller,
        @JsonProperty("category") final long category,
        @JsonProperty("extra") final String extra) {
        this.id = id;
        this.itemName = itemName;
        this.description = description;
        this.initialBid = initialBid;
        this.reserve = reserve;
        this.dateTime = dateTime;
        this.expires = expires;
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
            ", dateTime=" + dateTime +
            ", expires=" + expires +
            ", seller=" + seller +
            ", category=" + category +
            ", extra='" + extra + '\'' +
            '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Auction auction = (Auction) o;
        return id == auction.id
            && initialBid == auction.initialBid
            && reserve == auction.reserve
            && dateTime == auction.dateTime
            && expires == auction.expires
            && seller == auction.seller
            && category == auction.category
            && itemName.equals(auction.itemName)
            && description.equals(auction.description)
            && extra.equals(auction.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
            id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra);
    }

}
