package com.github.nexmark.kafka.queries;

import com.github.nexmark.kafka.model.Event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class JSONTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, final long partitionTime) {
        if (record.value() instanceof Event) {
            Event e = (Event) record.value();
            if (e.type == Event.Type.AUCTION) {
                return e.newAuction.dateTime.toEpochMilli();
            } else if (e.type == Event.Type.PERSON) {
                return e.newPerson.dateTime.toEpochMilli();
            } else if (e.type == Event.Type.BID) {
                return e.bid.dateTime.toEpochMilli();
            } else {
                throw new IllegalArgumentException("event type should be 0, 1 or 2; got " + e.type);
            }
        }
        throw new IllegalArgumentException("JsonTimestampExtractor cannot recognize the record value " + record.value());
    }

}