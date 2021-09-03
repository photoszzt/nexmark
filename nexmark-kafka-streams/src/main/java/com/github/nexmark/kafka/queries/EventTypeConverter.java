package com.github.nexmark.kafka.queries;
import com.google.gson.*;

import java.lang.reflect.Type;


import com.github.nexmark.kafka.model.Event;


public class EventTypeConverter implements JsonSerializer<Event.Type>, JsonDeserializer<Event.Type> {

    @Override
    public Event.Type deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        int val = json.getAsInt();
        if (val == 0) {
            return Event.Type.PERSON;
        } else if (val == 1) {
            return Event.Type.AUCTION;
        } else if (val == 2) {
            return Event.Type.BID;
        } else {
            throw new JsonParseException("event type should be 0,1 or 2");
        }
    }

    @Override
    public JsonElement serialize(Event.Type src, Type typeOfSrc, JsonSerializationContext context) {
        // TODO Auto-generated method stub
        return new JsonPrimitive(src.value);
    }
}
