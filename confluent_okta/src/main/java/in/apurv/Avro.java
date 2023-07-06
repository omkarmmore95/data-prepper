package in.apurv;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

public class Avro {

    private static Producer<String, GenericRecord> producer;
    private static KafkaConsumer<String, GenericRecord> consumer;
    private static CachedSchemaRegistryClient schemaRegistryClient;
    private static final String topic = "avro";
    private static String stringSchema;
    private static Schema schema;


    private static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }


        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        cfg.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        System.out.println(cfg);

        return cfg;
    }

    private static void init() throws Exception {
        Properties props = loadConfig("D:\\Apurv\\playground\\confluent_okta\\src\\main\\resources\\client.properties");
        producer = new KafkaProducer<>(props);
        consumer = new KafkaConsumer<>(props);
        Map prop = props;
        Map<String,String> propertyMap = (Map<String,String>) prop;
        schemaRegistryClient = new CachedSchemaRegistryClient(
                props.getProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
                100,propertyMap);
        stringSchema = schemaRegistryClient.getLatestSchemaMetadata(topic+"-value").getSchema();
        schema = Schema.parse(stringSchema);
    }


    public static void main(String[] args) throws Exception {
        init();

        new Thread(() -> {
            IntStream.range(1, 1000).forEach(i -> {
                System.out.println("producing " + i);
                try {
                    producer.send(new ProducerRecord<String, GenericRecord>
                            (topic,
                                    "key" + i,
                                    Converter.convertToGenericRecord(
                                            new SampleRecord(i,
                                                    Double.parseDouble("" + i),
                                                    "field3-" + i,
                                                    "field4-" + i),
                                            schema)), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if(null!=exception){
                                exception.printStackTrace();
                            }else{
                                System.out.println(metadata);
                            }
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("produced " + i);
            });
            producer.close();
        }).start();

        new Thread(() -> {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("key = %s, value = %s%n", record.key(), record.value());
                }
            }
        }).start();

        System.out.println("Hello world!");
    }
}