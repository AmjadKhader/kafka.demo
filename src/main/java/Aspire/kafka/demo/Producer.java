package Aspire.kafka.demo;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.clients.producer.*;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Properties;

@SpringBootApplication
public class Producer {

    public static void main(String[] args) {

        // Create logger for the class ..
        final Logger logger = LoggerFactory.getLogger(KafkaProperties.Producer.class);

        // Create Properties object for producer ..
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // setup which server to talk to ..
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer where both key and value are strings ..
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create Kafka Record ..
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>
                ("kafka-topic-demo-1", "demoKey", "demoValue");

        // Send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (exception == null) {
                    logger.info("record meta data \n" +
                            "Topic: " + recordMetadata.topic() + " , " +
                            "Partition: " + recordMetadata.partition());
                } else {
                    logger.error("Something went wrong ..");
                }
            }
        });

        // Close Connection ..
        producer.flush();
        producer.close();
    }

}
