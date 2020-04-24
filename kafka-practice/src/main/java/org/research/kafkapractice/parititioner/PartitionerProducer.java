package org.research.kafkapractice.parititioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * @fileName: PartitionerProducer.java
 * @description: PartitionerProducer.java类说明
 * @author: by echo huang
 * @date: 2020-04-24 17:52
 */
@Slf4j
@Component
public class PartitionerProducer {
    private static final KafkaProducer<String, String> partitionerProducer;

    static {
        partitionerProducer = new KafkaProducer<>(createProducerProp());
    }

    private static Properties createProducerProp() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return properties;
    }

    public void sendPartitioner(String msg, String topic) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "Last", msg);
        partitionerProducer.send(producerRecord, (metadata, exception) -> log.info("metaData partition:{}", metadata.partition()));
    }
}
