package com.github.nexmark.kafka.queries;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.UnsupportedEncodingException;
import java.util.Map;


public class LongSerdeWithDebug implements Serde<Long>, Deserializer<Long>, Serializer<Long> {

    @Override
    public void close() {}

    @Override
    public Deserializer<Long> deserializer() {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public Serializer<Long> serializer() {
        // TODO Auto-generated method stub
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Long data) {
        // TODO Auto-generated method stub
        if (data == null)
            return null;
        System.err.println("################# Serialize long data to bytes: " + data);

        return new byte[] {
            (byte) (data >>> 56),
            (byte) (data >>> 48),
            (byte) (data >>> 40),
            (byte) (data >>> 32),
            (byte) (data >>> 24),
            (byte) (data >>> 16),
            (byte) (data >>> 8),
            data.byteValue()
        };
    }

    @Override
    public Long deserialize(String topic, byte[] data) {
        String bytes_str = "";
        try {
            bytes_str = new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.err.println("################ long desc input is " + bytes_str);
        if (data == null)
            return null;
        if (data.length != 8) {
            throw new SerializationException("Size of data received by LongDeserializer is not 8, but " + data.length);
        }

        long value = 0;
        for (byte b : data) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }
    
}
