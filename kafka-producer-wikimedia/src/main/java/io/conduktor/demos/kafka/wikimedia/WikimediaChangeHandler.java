package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    public WikimediaChangeHandler(final KafkaProducer<String, String> kafkaProducer, final String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // nothing
    }

    @Override
    public void onClosed() throws Exception {
        log.info("Closed");
        kafkaProducer.close();
    }

    @Override
    public void onMessage(final String s, final MessageEvent messageEvent) throws Exception {
        String data = messageEvent.getData();
        log.info("data: {}", data);
        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(final String s) throws Exception {
        log.info("comment {}", s);
    }

    @Override
    public void onError(final Throwable throwable) {
        log.error("Error: {}", throwable.getMessage());
    }
}
