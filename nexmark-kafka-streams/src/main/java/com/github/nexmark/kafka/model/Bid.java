package com.github.nexmark.kafka.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class Bid implements Serializable {
    /**
     * Id of auction this bid is for.
     */
    public long auction; // foreign key: Auction.id

    /**
     * Id of person bidding in auction.
     */
    public long bidder; // foreign key: Person.id

    /**
     * Price of bid, in cents.
     */
    public long price;

    /**
     * The channel introduced this bidding.
     */
    public String channel;

    /**
     * The url of this bid.
     */
    public String url;

    /**
     * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
     * event time.
     */
    public long dateTime;

    /**
     * Additional arbitrary payload for performance testing.
     */
    public String extra;

    @JsonCreator
    public Bid(
        @JsonProperty("auction") final long auction,
        @JsonProperty("bidder") final long bidder,
        @JsonProperty("price") final long price,
        @JsonProperty("channel") final String channel,
        @JsonProperty("url") final String url,
        @JsonProperty("dateTime") final long dateTime,
        @JsonProperty("extra") final String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    @Override
    public boolean equals(final Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        final Bid other = (Bid) otherObject;
        return auction == other.auction
            && bidder == other.bidder
            && price == other.price
            && channel.equals(other.channel)
            && url.equals(other.url)
            && dateTime == other.dateTime
            && extra.equals(other.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(auction, bidder, price, channel, url, dateTime, extra);
    }

    @Override
    public String toString() {
        return "Bid{" +
            "auction=" + auction +
            ", bidder=" + bidder +
            ", price=" + price +
            ", channel=" + channel +
            ", url=" + url +
            ", dateTime=" + dateTime +
            ", extra='" + extra + '\'' +
            '}';
    }
}
