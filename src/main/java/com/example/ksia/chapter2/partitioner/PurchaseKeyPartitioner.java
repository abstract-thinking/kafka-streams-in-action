package com.example.ksia.chapter2.partitioner;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import com.example.ksia.model.PurchaseKey;

public class PurchaseKeyPartitioner extends DefaultPartitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String newKey = null;
        if (key != null) {
            PurchaseKey purchaseKey = (PurchaseKey) key;
            newKey = purchaseKey.getCustomerId();
            keyBytes = newKey.getBytes();
        }

        return super.partition(topic, newKey, keyBytes, value, valueBytes, cluster);
    }
}
