package kasyan;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        String topic = "wikimedia.recent_change";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        final Thread main = Thread.currentThread();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("shutdown: " + Thread.currentThread());
                consumer.wakeup();
                try {
                    main.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));
            // subscribe
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(r -> {
                    LOG.info("Key: {}, value: {}", r.key(), r.value());
                    LOG.info("Partition: {}, offset: {}", r.partition(), r.offset());
                });
            }
        } catch (WakeupException e) {
            // ignore
            LOG.info("Wake up exception");
        } catch (Exception e) {
            LOG.error("Exception {}", e.getMessage());
        }
    }
}
