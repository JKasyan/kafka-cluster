package kasyan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class Producer {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        LOG.info("Producer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Config.PROPERTIES)) {
            var k = "key1";
            LOG.info("partition {}", k.hashCode() % 2);
            var record = new ProducerRecord<>(
                    "second_topic",
                    "key1",
                    String.valueOf(Instant.now().getEpochSecond())
            );
            producer.send(record);
            producer.flush();
            LOG.info("sent");
        }
    }
}
