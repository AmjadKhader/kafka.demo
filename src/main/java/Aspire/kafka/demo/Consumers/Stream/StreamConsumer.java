package Aspire.kafka.demo.Consumers.Stream;

import Aspire.kafka.demo.Consumers.Springboot.KafkaConsumerSpringbootApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.internals.KStreamFlatTransform;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class StreamConsumer {
    public static void main(String[] args) {

        SpringApplication application = new SpringApplication(StreamConsumer.class);
        application.setDefaultProperties(Collections.singletonMap("server.port", "8083"));
        application.run(args);

        //setup prop ..
        Properties properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "DemoStreamConsumer");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kStream = streamsBuilder.stream("demo2");
        kStream.foreach((headers, value) -> System.out.println("key: " + headers + " and value: " + value));

        try (KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties)) {
            streams.start();
        } catch (Exception e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
        }
    }
}