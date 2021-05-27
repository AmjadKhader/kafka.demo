package Aspire.kafka.demo.Consumers.StreamListener;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableBinding(Sink.class)
public class StreamListenerDemo {

    @StreamListener("input")
    public void consumerMessages(String message) {
        System.out.println("\n \n \n " + message + "\n \n \n ");
    }

    public static void main(String[] args) {

        SpringApplication application = new SpringApplication(StreamListenerDemo.class);
        application.setDefaultProperties(Collections.singletonMap("server.port", "8082"));
        application.run(args);
    }
}