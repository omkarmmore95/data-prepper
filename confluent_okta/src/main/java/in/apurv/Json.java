package in.apurv;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
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

@Slf4j
public class Json {

    private static Producer<String, SampleRecord> producer;
    private static KafkaConsumer<String, JsonNode> consumer;
    private static final String topic = "json";



    private static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }



        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);

        return cfg;
    }

    private static void init() throws IOException {
        Properties props = loadConfig("D:\\Apurv\\playground\\confluent_okta\\src\\main\\resources\\client.properties");
        producer = new KafkaProducer<>(props);
        consumer = new KafkaConsumer<>(props);
    }


    public static void main(String[] args) throws Exception {
        init();

        new Thread(() -> {
            IntStream.range(1, 1000).forEach(i -> {
                System.out.println("producing "+ i);
                producer.send(new ProducerRecord<String, SampleRecord>
                                (topic,
                                        "key" + i,
                                        new SampleRecord(i, Double.parseDouble("" + i), "field3-" + i, "field4-" + i))
                        , new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if(exception!=null){
                                    exception.printStackTrace();
                                }else{
                                    System.out.println(metadata);
                                }
                            }
                        });
                System.out.println("produced "+ i);
            });
            producer.close();
        }).start();

        new Thread(() -> {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, JsonNode> record : records) {
                    System.out.printf("key = %s, value = %s%n", record.key(), record.value());
                }
            }
        }).start();

        System.out.println("Hello world!");
    }
}