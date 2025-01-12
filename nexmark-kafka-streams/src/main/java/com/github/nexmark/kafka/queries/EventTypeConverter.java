package com.github.nexmark.kafka.queries;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;


import com.github.nexmark.kafka.model.Event;


public class EventTypeConverter implements JsonSerializer<Event.EType>, JsonDeserializer<Event.EType> {

    @Override
    public Event.EType deserialize(final JsonElement json, final Type typeOfT,
                                   final JsonDeserializationContext context) throws JsonParseException {
        final int val = json.getAsInt();
        if (val == 0) {
            return Event.EType.PERSON;
        } else if (val == 1) {
            return Event.EType.AUCTION;
        } else if (val == 2) {
            return Event.EType.BID;
        } else {
            throw new JsonParseException("event type should be 0,1 or 2");
        }
    }

    @Override
    public JsonElement serialize(final Event.EType src, final Type typeOfSrc, final JsonSerializationContext context) {
        return new JsonPrimitive(src.value);
    }
}
