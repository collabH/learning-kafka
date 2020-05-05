package org.research.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.util.Lists;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;

/**
 * @fileName: BroadCastConsumer.java
 * @description: BroadCastConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-05-05 17:22
 */
@Component
public class BroadCastConsumer {
    public static final KafkaConsumer<String, String> KAFKA_CONSUMER;
    public static final KafkaConsumer<String, String> KAFKA_CONSUMER1;

    static {
        KAFKA_CONSUMER = new KafkaConsumer<>(createConsumerProperties());
        KAFKA_CONSUMER1 = new KafkaConsumer<>(createConsumerProperties1());
    }

    /**
     * 创建消费者配置
     *
     * @return
     */
    private static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "learning-kafka-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "origin-consumer");
        return properties;
    }

    /**
     * 创建消费者配置
     *
     * @return
     */
    private static Properties createConsumerProperties1() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "learning-kafka-consumer-group1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "origin-consumer1");
        return properties;
    }

    public void consumer(){
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("test-topic"));
        KAFKA_CONSUMER1.subscribe(Lists.newArrayList("test-topic"));


        while (true){
            ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofSeconds(2));
            ConsumerRecords<String, String> poll1 = KAFKA_CONSUMER1.poll(Duration.ofSeconds(2));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            Iterator<ConsumerRecord<String, String>> iterator1 = poll1.iterator();

            if (iterator.hasNext()) {
                System.out.println(iterator.next().value());
            }
            if (iterator1.hasNext()){
                System.out.println(iterator1.next().value());
            }
        }
    }
}
