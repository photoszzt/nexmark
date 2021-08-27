package com.github.nexmark.kafka.queries;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class MsgPOJOSerde<T extends JSONSerdeCompatible> implements Deserializer<T>, Serializer<T>, Serde<T> {
    private ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    public MsgPOJOSerde() {

    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        T data;
        try {
            return (T) objectMapper.readValue(bytes, JSONSerdeCompatible.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing MSGPACK message", e);
        }
    }

    @Override
    public void close() {}

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
