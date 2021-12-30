package kafka.demo.consumers.springboot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

@Component
public class Consumer {

    @KafkaListener(topics = "demo2",
            groupId = "test-consumer-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> consumerRecord)
    {
        log.println("Received Message: " + consumerRecord.headers().toString());
        log.println("Value: " + consumerRecord.value());
    }
}