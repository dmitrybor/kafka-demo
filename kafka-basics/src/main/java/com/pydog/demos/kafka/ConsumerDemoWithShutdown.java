package com.pydog.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        final String bootstrapServer = "localhost:9092";
        final String consumerGroupId = "my-consumer-app-2";
        final String topic = "demo_java";

        LOGGER.info("Creating consumer config...");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        LOGGER.info("Creating consumer...");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Detected a shutdown. Exiting by calling consumer.wakeup().");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));


        LOGGER.info("Subscribing consumer to topic...");
        consumer.subscribe(Collections.singleton(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                consumerRecords.forEach(record ->
                        LOGGER.info("Key: {}, Value: {},\nPartition: {}, Offset: {}",
                                record.key(), record.value(), record.partition(), record.offset()
                        ));


            }
        } catch (WakeupException e) {
            LOGGER.info("Wakeup exception. This is expected when closing the consumer.");
        } catch (Exception e) {
            LOGGER.error("Unexpected exception.", e);
        } finally {
            consumer.close();
            LOGGER.info("The consumer is now gracefully closed.");
        }
    }
}
