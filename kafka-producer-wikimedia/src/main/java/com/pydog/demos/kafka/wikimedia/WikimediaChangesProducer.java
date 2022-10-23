package com.pydog.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import service.WikimediaEventHandler;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        final String bootstrapServer = "localhost:9092";
        final String topicName = "wikimedia.recentchanges";
        final String wikimediaEventStreamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Ensure producer safety
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // Optimize for high throughput
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> recentChangesProducer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaEventHandler(recentChangesProducer, topicName);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(eventHandler, URI.create(wikimediaEventStreamUrl));
        try (EventSource eventSource = eventSourceBuilder.build() ) {
            eventSource.start();
            TimeUnit.MINUTES.sleep(10);
        }
    }
}
