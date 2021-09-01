package com.github.nexmark.kafka.queries;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nexmark.kafka.model.Event;

import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class JSONPOJOSerde<T extends JSONSerdeCompatible> implements Deserializer<T>, Serializer<T>, Serde<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> type;

    public JSONPOJOSerde(Class<T> type) {
        this.type = type;
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
            System.out.println("Input is " + new String(bytes, "UTF-8"));
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
            byte[] ret =  objectMapper.writeValueAsBytes(data);
            System.out.println("serial to " + new String(ret, "UTF-8"));
            return ret;
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
