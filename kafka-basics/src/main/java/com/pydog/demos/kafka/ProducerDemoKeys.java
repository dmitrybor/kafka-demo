package com.pydog.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        LOGGER.info("Creating producer config...");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        LOGGER.info("Creating producer...");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        LOGGER.info("Sending data to broker asynchronously...");
        for (int i = 0; i < 10; i++) {
            String topic = "demo_java";
            String value = "Hello world! #" + i;
            String key = "id_" + i;

            LOGGER.info("Creating producer record...");
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            producer.send(producerRecord, (metadata, exception) -> {
                        if (exception == null) {
                            LOGGER.info("Received new metadata: \n" +
                                            "Topic: {}\n" +
                                            "Key: {}\n" +
                                            "Partition: {}\n" +
                                            "Offset: {}\n" +
                                            "Timestamp: {}",
                                    metadata.topic(),
                                    producerRecord.key(),
                                    metadata.partition(),
                                    metadata.offset(),
                                    metadata.timestamp()
                            );
                        } else {
                            LOGGER.error("Error while producing.", exception);
                        }
                    }
            );
        }

        LOGGER.info("Flushing data synchronously...");
        producer.flush();

        LOGGER.info("Closing producer (will also flush data)...");
        producer.close();
    }
}
