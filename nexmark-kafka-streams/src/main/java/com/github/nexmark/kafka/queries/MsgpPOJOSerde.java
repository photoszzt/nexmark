package com.github.nexmark.kafka.queries;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.msgpack.jackson.dataformat.MessagePackMapper;
import com.github.nexmark.kafka.model.Event;

import java.io.IOException;
import java.util.Map;

public class MsgpPOJOSerde<T> implements Deserializer<T>, Serializer<T>, Serde<T> {
    private final ObjectMapper objectMapper;
    private Class<T> cls;
    public MsgpPOJOSerde() {
        objectMapper = new MessagePackMapper();
        final SimpleModule eventDescMod = new SimpleModule();
        eventDescMod.addDeserializer(Event.class, new EventMsgpDeserialzer());
        objectMapper.registerModule(eventDescMod);
        objectMapper.registerModule(new JavaTimeModule());
    }

    public void setClass(final Class<T> cls) {
        this.cls = cls;
    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {}

    @Override
    public void close() {}

    @Override
    public T deserialize(final String s, final byte[] bytes) {
        try {
            assert bytes != null;
            final T obj = objectMapper.readValue(bytes, this.cls);
            assert obj != null;
            return obj;
        } catch (final IOException e) {
            e.printStackTrace();
            throw new SerializationException(e);
        }
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public byte[] serialize(final String s, final T t) {
        try {
            return this.objectMapper.writeValueAsBytes(t);
        } catch (final JsonProcessingException e) {
            e.printStackTrace();
            throw new SerializationException(e);
        }
    }
}
