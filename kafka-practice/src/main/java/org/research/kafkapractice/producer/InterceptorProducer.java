package org.research.kafkapractice.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.research.kafkapractice.interceptor.MessageInterceptor;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * @fileName: InterceptorProducer.java
 * @description: InterceptorProducer.java类说明
 * @author: by echo huang
 * @date: 2020-05-10 16:00
 */
@Component
public class InterceptorProducer {
    public void send() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MessageInterceptor.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024000);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024000 * 4);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000 * 4);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "hello");
        producer.send(record);
    }
}
