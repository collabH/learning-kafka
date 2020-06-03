package org.research.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @fileName: ExactlyOnceConsumer.java
 * @description: ExactlyOnceConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-06-02 16:43
 */
public class ExactlyOnceConsumer {
    private static final AtomicLong OFFSET_STORE = new AtomicLong(1L);
    private static final KafkaConsumer<String, String> KAFKA_CONSUMER;

    static {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "offset_group");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset_client");
        KAFKA_CONSUMER = new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) {
        TopicPartition topicPartition = new TopicPartition("list_topic", 0);
        KAFKA_CONSUMER.seek(topicPartition, OFFSET_STORE.get());
        ConsumerRecords<String, String> records = KAFKA_CONSUMER.poll(Duration.ofSeconds(1));

        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while (true) {
            ConsumerRecord<String, String> next = iterator.next();
            System.out.println(next.value());
        }

    }
}
