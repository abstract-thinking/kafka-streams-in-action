/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.ksia.chapter3;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import com.example.ksia.client.producer.MockDataProducer;

public class KafkaStreamsYellingApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsYellingApp.class);

    private static Properties getAdminConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        return properties;
    }

    private static void generateTopics() {
        AdminClient adminClient = AdminClient.create(getAdminConfig());
        //new NewTopic(topicName, numPartitions, replicationFactor)
        NewTopic newTopic = new NewTopic("src-topic", 1, (short)1);
        NewTopic newTopic2 = new NewTopic("out-topic", 1, (short)1);
        adminClient.createTopics(List.of(newTopic, newTopic2));
        adminClient.close();
    }

    private static Properties getConfiguration() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        return props;
    }

    public static void main(String[] args) throws Exception {
        generateTopics();

        StreamsConfig streamsConfig = new StreamsConfig(getConfiguration());
        Topology topology = createTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);

        LOG.info("Hello World Yelling App Started");
        //Used only to produce data for this application, not typical usage
        MockDataProducer.produceRandomTextData();
        kafkaStreams.start();
        Thread.sleep(35000);
        LOG.info("Shutting down the Yelling APP now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Topology createTopology() {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> simpleFirstStream = builder.stream("src-topic", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues((ValueMapper<String, String>) String::toUpperCase);
        upperCasedStream.to( "out-topic", Produced.with(stringSerde, stringSerde));
        upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("Yelling App"));

        return builder.build();
    }
}