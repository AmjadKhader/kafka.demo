package kafka.demo.Consumers.Springboot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

    @KafkaListener(topics = "demo2",
            groupId = "test-consumer-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> consumerRecord)
    {
        System.out.println("Received Message: " + consumerRecord.headers().toString());
        System.out.println("Value: " + consumerRecord.value());
    }
}