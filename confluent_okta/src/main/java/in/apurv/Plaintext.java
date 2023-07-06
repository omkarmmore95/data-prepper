package in.apurv;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

public class Plaintext {

    private static Properties props;
    private static Producer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;

    private static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        return cfg;
    }

    private static void init() throws IOException {
        props = loadConfig("D:\\Apurv\\playground\\confluent_okta\\src\\main\\resources\\client.properties");
        producer = new KafkaProducer<>(props);
        consumer = new KafkaConsumer<>(props);
    }


    public static void main(String[] args) throws Exception {
        init();

        new Thread(() -> {
            IntStream.range(1, 1000).forEach(i -> {
                System.out.println("producing "+ i);
                producer.send(new ProducerRecord<>("plaintext", "key" + i, "value" + i));
                System.out.println("produced "+ i);
            });
            producer.close();
        }).start();

        new Thread(() -> {
            consumer.subscribe(List.of("plaintext"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("key = %s, value = %s%n", record.key(), record.value());
                }
            }
        }).start();

        System.out.println("Hello world!");
    }
}