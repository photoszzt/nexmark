package com.github.nexmark.kafka.queries;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.time.Instant;

public class InstantTypeConverter implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
    @Override
    public JsonElement serialize(final Instant src, final Type srcType, final JsonSerializationContext context) {
        return new JsonPrimitive(src.toEpochMilli());
    }

    @Override
    public Instant deserialize(final JsonElement json, final Type type, final JsonDeserializationContext context)
            throws JsonParseException {
        return Instant.ofEpochMilli(json.getAsLong());
    }
}

