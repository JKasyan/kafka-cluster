package kasyan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

public class ProducerCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerCallback.class);

    public static void main(String[] args) {
        try (var producer = new KafkaProducer<String, String>(Config.PROPERTIES)) {
            var k = "key1";
            IntStream.range(0, 10_000).forEach(d -> {
                var record = new ProducerRecord<>(
                        "first_topic",
                        "key" + d,
                        String.valueOf(d)
                );
                producer.send(record, (rm, e) -> {
                    if (e != null) {
                        LOG.error("error {}", e.getMessage());
                    } else {
                        LOG.info("Successfully sent");
                        LOG.info("Topic {}, partition {}, offset {}", rm.topic(), rm.partition(), rm.offset());
                    }
                });
            });
            producer.flush();
            LOG.info("sent");
        }
    }
}
