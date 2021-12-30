package kafka.demo.consumers.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerSpringbootApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerSpringbootApplication.class, args);
    }
}
