package Aspire.kafka.demo;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {

        // Create logger for the class ..
        final Logger logger = LoggerFactory.getLogger(KafkaProperties.Consumer.class);

        // Create Variables for the properties ..
        String bootStrapServer = "127.0.0.1:9092";
        String consumerGroupID = "test-consumer-group"; // single default group

        // Create properties object ..
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // if there is no current offset, this will automatically reset the offset to the earliest

        // Create Kafka Consumer ..
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a topic ..
        consumer.subscribe(Collections.singletonList("kafka-topic-demo-1"));

        // keep reading records ..
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord consumerRecord : consumerRecords) {
                logger.info("New record received .. \n" +
                        " Key: " + consumerRecord.key() +
                        " Value: " + consumerRecord.value() +
                        " Topic: " + consumerRecord.topic() +
                        " Partition: " + consumerRecord.partition()
                );
            }
        }
    }
}
