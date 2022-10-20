package com.pydog.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        LOGGER.info("Creating producer config...");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        LOGGER.info("Creating producer...");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        LOGGER.info("Creating producer record...");
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "Hello World!");

        LOGGER.info("Sending data to broker asynchronously...");
        producer.send(producerRecord);

        LOGGER.info("Flushing data synchronously...");
        producer.flush();

        LOGGER.info("Closing producer (will also flush data)...");
        producer.close();
    }
}
