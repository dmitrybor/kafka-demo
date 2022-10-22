package service;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaEventHandler implements EventHandler {
    private final static Logger LOGGER = LoggerFactory.getLogger(WikimediaEventHandler.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;

    public WikimediaEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        String messageData = messageEvent.getData();
        LOGGER.info("Message received: {}", messageData);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageData);
        kafkaProducer.send(producerRecord);
    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        LOGGER.error("Error while handling a message.", t);
    }
}
