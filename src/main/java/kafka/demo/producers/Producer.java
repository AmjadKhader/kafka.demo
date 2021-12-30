package kafka.demo.producers;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.clients.producer.*;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.ArrayList;
import java.util.Properties;

@SpringBootApplication
public class Producer {

    public static void main(String[] args) {

        // Create logger for the class ..
        Logger logger = LoggerFactory.getLogger(KafkaProperties.Producer.class);

        // Create Properties object for producer ..
        Properties properties = new Properties();

        // setup which server to talk to ..
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer where both key and value are strings ..
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create Kafka Record ..
        Header header = new Header() {
            @Override
            public String key() {
                return "my header";
            }

            @Override
            public byte[] value() {
                return new byte[0];
            }
        };

        ArrayList<Header> headers = new ArrayList<>();
        headers.add(header);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>
                ("demo2", (Integer) null, "demoKey", "demoValue", headers);

        // Send data
        producer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                String message = "\n record meta data \n" +
                        "Topic: " + recordMetadata.topic() + " , " +
                        "Partition: " + recordMetadata.partition();

                logger.info(message);
            } else {
                logger.error("Something went wrong ..");
            }
        });

        // Close Connection ..
        producer.flush();
        producer.close();
    }
}
