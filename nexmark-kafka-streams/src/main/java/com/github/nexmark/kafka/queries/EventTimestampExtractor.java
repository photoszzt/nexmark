package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class EventTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
        if (record.value() instanceof Event) {
            Event e = (Event) record.value();
            if (e.etype == Event.EType.AUCTION) {
                return e.newAuction.dateTime;
            } else if (e.etype == Event.EType.PERSON) {
                return e.newPerson.dateTime;
            } else if (e.etype == Event.EType.BID) {
                return e.bid.dateTime;
            } else {
                throw new IllegalArgumentException("event type should be 0, 1 or 2; got " + e.etype);
            }
        }
        throw new IllegalArgumentException("EventTimestampExtractor cannot recognize the record value " + record.value());
    }
}