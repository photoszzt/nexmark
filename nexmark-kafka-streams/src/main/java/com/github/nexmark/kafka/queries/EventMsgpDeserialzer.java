package com.github.nexmark.kafka.queries;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.github.nexmark.kafka.model.Auction;
import com.github.nexmark.kafka.model.Bid;
import com.github.nexmark.kafka.model.Event;
import com.github.nexmark.kafka.model.Person;

import java.io.IOException;

public class EventMsgpDeserialzer extends StdDeserializer<Event> {

    public EventMsgpDeserialzer() {
        this(null);
    }

    public EventMsgpDeserialzer(final Class<?> vc) {
        super(vc);
    }

    @Override
    public Event deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
        final JsonNode node = p.getCodec().readTree(p);
        if (node == null) {
            throw new RuntimeException("event should not be null");
        }
        final JsonNode etypeNode = node.get("etype");
        if (etypeNode == null) {
            throw new RuntimeException("event should have a etype");
        }
        long injTsMs = 0;
        final JsonNode injTsMsNode = node.get("injTsMs");
        if (injTsMsNode != null) {
            injTsMs = injTsMsNode.asLong();
        }
        final byte etype = (byte) node.get("etype").asInt();
        if (etype == 0) {
            final JsonNode personNode = node.get("newPerson");
            if (personNode == null) {
                throw new RuntimeException("etype is 0; but no person is enclosed");
            }
            final long id = personNode.get("id").asLong();
            final String name = personNode.get("name").asText();
            final String emailAddress = personNode.get("emailAddress").asText();
            final String creditCard = personNode.get("creditCard").asText();
            final String city = personNode.get("city").asText();
            final String state = personNode.get("state").asText();
            final long dateTime = personNode.get("dateTime").asLong();
            final String extra = personNode.get("extra").asText();
            final Person newPerson = new Person(id, name, emailAddress, creditCard, city, state, dateTime, extra);
            return new Event(newPerson, injTsMs);
        } else if (etype == 1) {
            final JsonNode auctionNode = node.get("newAuction");
            if (auctionNode == null) {
                throw new RuntimeException("etype is 1; but no auction is enclosed");
            }
            final long id = auctionNode.get("id").asLong();
            final String itemName = auctionNode.get("itemName").asText();
            final String description = auctionNode.get("description").asText();
            final long initialBid = auctionNode.get("initialBid").asLong();
            final long reserve = auctionNode.get("reserve").asLong();
            final long dateTime = auctionNode.get("dateTime").asLong();
            final long expires = auctionNode.get("expires").asLong();
            final long seller = auctionNode.get("seller").asLong();
            final long category = auctionNode.get("category").asLong();
            final String extra = auctionNode.get("extra").asText();
            final Auction newAuction = new Auction(id, itemName, description, initialBid, reserve, dateTime,
                expires, seller, category, extra);
            return new Event(newAuction, injTsMs);
        } else if (etype == 2) {
            final JsonNode bidNode = node.get("bid");
            if (bidNode == null) {
                throw new RuntimeException("etype is 2; but no bid is enclosed");
            }
            final long auction = bidNode.get("auction").asLong();
            final long bidder = bidNode.get("bidder").asLong();
            final long price = bidNode.get("price").asLong();
            final String channel = bidNode.get("channel").asText();
            final String url = bidNode.get("url").asText();
            final long dateTime = bidNode.get("dateTime").asLong();
            final String extra = bidNode.get("extra").asText();
            final Bid bid = new Bid(auction, bidder, price, channel, url, dateTime, extra);
            return new Event(bid, injTsMs);
        } else {
            throw new RuntimeException("etype should be either 0, 1 or 2; got " + etype);
        }
    }
}
