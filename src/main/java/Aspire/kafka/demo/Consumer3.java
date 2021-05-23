package Aspire.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer3 extends Thread {

    // Create properties object ..
    private static final Properties properties = new Properties();

    // Create Kafka Consumer ..
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Create logger for the class ..
    final Logger logger = LoggerFactory.getLogger(KafkaProperties.Consumer.class);

    public static void main(String[] args) {

        // Create Variables for the properties ..
        String bootStrapServer = "127.0.0.1:9092";
        String consumerGroupID = "test-consumer-group"; // single default group

        // Create properties object ..
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // if there is no current offset, this will automatically reset the offset to the earliest
        //**properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer3 consumer = new Consumer3();
        consumer.run();
    }

    @Override
    public void run() {

        try {
            // Subscribe to a topic ..
            consumer.subscribe(Collections.singletonList("kafka-topic-demo-2"));

            // keep reading records ..
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    String data = "\n New record received .. \n" +
                            " Value: " + consumerRecord.value() +
                            " Topic: " + consumerRecord.topic() +
                            " Partition: " + consumerRecord.partition();

                    logger.info(data);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
