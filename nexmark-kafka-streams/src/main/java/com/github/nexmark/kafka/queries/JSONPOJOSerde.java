package com.github.nexmark.kafka.queries;

import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.github.nexmark.kafka.model.Event;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;
import java.util.Map;

public class JSONPOJOSerde<T> implements Deserializer<T>, Serializer<T>, Serde<T> {
    private final Gson gson;
    private Class<T> cls;

    public JSONPOJOSerde() {
        InstantTypeConverter serdeInstant = new InstantTypeConverter();
        EventTypeConverter serdeEventType = new EventTypeConverter();
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Instant.class, serdeInstant);
        gsonBuilder.registerTypeAdapter(Event.EType.class, serdeEventType);

        gson = gsonBuilder.create();
    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
    }

    public void setClass(final Class<T> cls) {
        this.cls = cls;
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        
        try {
            String bytes_str = new String(bytes, "UTF-8");
            // System.out.println("########## Input is " + bytes_str);
            if (this.cls == null) {
                throw new SerializationException("need to call setClass to pass the type to JSONPOJOSerde");
            }
            return this.gson.fromJson(bytes_str, this.cls);
            // System.out.println("########### Desc out is " + desc.toString());
            // return desc;
        } catch (Exception err) {
            throw new SerializationException(err);
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }
        try {
            final String ret = this.gson.toJson(data);
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
