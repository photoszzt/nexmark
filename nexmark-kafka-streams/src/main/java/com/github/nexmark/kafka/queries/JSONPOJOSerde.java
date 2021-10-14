package com.github.nexmark.kafka.queries;

import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.github.nexmark.kafka.model.Event;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.reflect.TypeUtils;
import java.time.Instant;
import java.util.Map;

public class JSONPOJOSerde<T> implements Deserializer<T>, Serializer<T>, Serde<T> {
    private Gson gson;

    public JSONPOJOSerde() {
        InstantTypeConverter serdeInstant = new InstantTypeConverter();
        EventTypeConverter serdeEventType = new EventTypeConverter();
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Instant.class, serdeInstant);
        gsonBuilder.registerTypeAdapter(Event.Type.class, serdeEventType);

        gson = gsonBuilder.create();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    public Class<T> returnedClass() throws ClassNotFoundException {
        return (Class<T>) TypeUtils.getTypeArguments(getClass(), JSONPOJOSerde.class)
                .get(JSONPOJOSerde.class.getTypeParameters()[0]);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        
        try {
            String bytes_str = new String(bytes, "UTF-8");
            // System.out.println("Input is " + bytes_str);
            T desc = (T)this.gson.fromJson(bytes_str, returnedClass());
            // System.out.println("desc out is " + desc.toString());
            return desc;
        } catch (Exception err) {
            throw new SerializationException(err);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            String ret = this.gson.toJson(data);
            return ret.getBytes();
        } catch (Exception err) {
            throw new SerializationException("Error serializing JSON message", err);
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
