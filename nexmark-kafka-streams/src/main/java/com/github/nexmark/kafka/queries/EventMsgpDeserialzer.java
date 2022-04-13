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
import java.time.Instant;

public class EventMsgpDeserialzer extends StdDeserializer<Event> {

    public EventMsgpDeserialzer() {
        this(null);
    }

    public EventMsgpDeserialzer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Event deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        if (node == null) {
            return null;
        }
        JsonNode etypeNode = node.get("etype");
        if (etypeNode == null) {
            return null;
        }
        byte etype = (byte)node.get("etype").asInt();
        if (etype == 0) {
            JsonNode personNode = node.get("newPerson");
            if (personNode == null) {
                throw new RuntimeException("etype is 0; but no person is enclosed");
            }
            long id = personNode.get("id").asLong();
            String name = personNode.get("name").asText();
            String emailAddress = personNode.get("emailAddress").asText();
            String creditCard = personNode.get("creditCard").asText();
            String city = personNode.get("city").asText();
            String state = personNode.get("state").asText();
            Instant dateTime = Instant.ofEpochMilli(personNode.get("dateTime").asLong());
            String extra = personNode.get("extra").asText();
            Person newPerson = new Person(id, name, emailAddress, creditCard, city, state, dateTime, extra);
            return new Event(newPerson);
        } else if (etype == 1) {
            JsonNode auctionNode = node.get("newAuction");
            if (auctionNode == null) {
                throw new RuntimeException("etype is 1; but no auction is enclosed");
            }
            long id = auctionNode.get("id").asLong();
            String itemName = auctionNode.get("itemName").asText();
            String description = auctionNode.get("description").asText();
            long initialBid = auctionNode.get("initialBid").asLong();
            long reserve = auctionNode.get("reserve").asLong();
            Instant dateTime = Instant.ofEpochMilli(auctionNode.get("dateTime").asLong());
            Instant expires = Instant.ofEpochMilli(auctionNode.get("expires").asLong());
            long seller = auctionNode.get("seller").asLong();
            long category = auctionNode.get("category").asLong();
            String extra = auctionNode.get("extra").asText();
            Auction newAuction = new Auction(id, itemName, description, initialBid, reserve, dateTime,
                    expires, seller, category, extra);
            return new Event(newAuction);
        } else if (etype == 2) {
            JsonNode bidNode = node.get("bid");
            if (bidNode == null) {
                throw new RuntimeException("etype is 2; but no bid is enclosed");
            }
            long auction = bidNode.get("auction").asLong();
            long bidder = bidNode.get("bidder").asLong();
            long price = bidNode.get("price").asLong();
            String channel = bidNode.get("channel").asText();
            String url = bidNode.get("url").asText();
            Instant dateTime = Instant.ofEpochMilli(bidNode.get("dateTime").asLong());
            String extra = bidNode.get("extra").asText();
            Bid bid = new Bid(auction, bidder, price, channel, url, dateTime, extra);
            return new Event(bid);
        } else {
            throw new RuntimeException("etype should be either 0, 1 or 2; got " + etype);
        }
    }
}
