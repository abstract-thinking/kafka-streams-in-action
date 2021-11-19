package com.example.ksia.chapter2.producer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.example.ksia.chapter2.partitioner.PurchaseKeyPartitioner;
import com.example.ksia.model.PurchaseKey;

/**
 * Example of a simple producer, not meant to run as a stand alone example.
 *
 * If desired to run this example change the ProducerRecord below to
 * use a real topic name and comment out line #33 below.
 */
public class SimpleProducer {
    public static final int NUMBER_OF_PARTITIONS = 2;

    private static Properties getAdminConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        return properties;
    }

    private static Properties getProducerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        // properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", PurchaseKeySerializer.class);
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");
        //This line in for demonstration purposes
        properties.put("partitioner.class", PurchaseKeyPartitioner.class.getName());

        return properties;
    }

    public static void main(String[] args) {
        generateTopic();

        PurchaseKey key = new PurchaseKey("12334568", new Date());

        try(Producer<PurchaseKey, String> producer = new KafkaProducer<>(getProducerConfig())) {
            ProducerRecord<PurchaseKey, String> record = new ProducerRecord<>("some-topic", key, "value");
            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            Future<RecordMetadata> sendFuture = producer.send(record, callback);
        }

    }

    private static void generateTopic() {
        AdminClient adminClient = AdminClient.create(getAdminConfig());
        //new NewTopic(topicName, numPartitions, replicationFactor)
        NewTopic newTopic = new NewTopic("some-topic", NUMBER_OF_PARTITIONS, (short)1);
        adminClient.createTopics(List.of(newTopic));
        adminClient.close();
    }
}