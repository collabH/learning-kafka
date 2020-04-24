package org.research.kafkapractice.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @fileName: OriginalProducer.java
 * @description: 原始Kafka生产者
 * @author: by echo huang
 * @date: 2020-04-24 11:32
 */
@Component
@Slf4j
public class OriginalProducer {

    private static final KafkaProducer<String, String> originalProducer;

    static {
        originalProducer = new KafkaProducer<>(createProducerProp());
    }

    private static Properties createProducerProp() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }


    /**
     * 同步发送消息
     *
     * @param msg
     * @param topic
     */
    public void syncSendMsg(String msg, String topic) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
        try {
            RecordMetadata recordMetadata = originalProducer.send(producerRecord).get();
            log.info("metaData:{}", recordMetadata);
        } catch (InterruptedException e) {
            log.error("消息发送失败");
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步发送消息
     *
     * @param msg
     * @param topic
     */
    public void asyncSendMsg(String msg, String topic) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
        originalProducer.send(producerRecord, (metadata, exception) -> log.info("metaData:{}", metadata));
    }
}
