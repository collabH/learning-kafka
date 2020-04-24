package org.research.kafkapractice.serializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @fileName: AvroSerializer.java
 * @description: avro序列化器
 * @author: by echo huang
 * @date: 2020-04-24 16:06
 */
public class AvroSerializer {
    private static final String schema;

    static {
        schema = "{\n" +
                "\t\"namespace\":\"person.avro\",\n" +
                "\t\"type\":\"record\",\n" +
                "\t\"name\":\"Person\",\n" +
                "\t\"fields\":[\n" +
                "\t\t{\"name\":\"id\",\"type\":\"int\"},\n" +
                "\t\t{\"name\":\"name\",\"type\":\"string\"},\n" +
                "\t\t{\"name\":\"sex\",\"type\":\"string\"}\n" +
                "\t]\n" +
                "}";
    }

    private static final KafkaProducer<String, String> originalProducer;

    static {
        originalProducer = new KafkaProducer<>(createProducerProp());
    }

    private static Properties createProducerProp() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url","s");
        return properties;
    }




}
