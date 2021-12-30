package kafka.demo.Consumers.StreamListener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

import java.util.Collections;

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