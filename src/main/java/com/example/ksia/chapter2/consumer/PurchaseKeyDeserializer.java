package com.example.ksia.chapter2.consumer;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.example.ksia.model.PurchaseKey;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PurchaseKeyDeserializer implements Deserializer<PurchaseKey> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public PurchaseKey deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing...");
            return objectMapper.readValue(new String(data, "UTF-8"), PurchaseKey.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to PurchaseKey");
        }
    }

    @Override
    public void close() {
    }
}
