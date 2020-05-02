package org.research.kafkapractice.consumer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @fileName: StandAloneConsumer.java
 * @description: StandAloneConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-04-27 16:59
 */
@Slf4j
@Component
public class StandAloneConsumer {
    private static final KafkaConsumer<String, String> STANDALONE_CONSUMER;

    private static final List<TopicPartition> partitions = Lists.newArrayList();

    static {
        STANDALONE_CONSUMER = new KafkaConsumer<>(createConsumerProperties());
    }

    /**
     * 创建消费者配置
     *
     * @return
     */
    private static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    public void consumer() {
        List<PartitionInfo> partitionInfos = STANDALONE_CONSUMER.partitionsFor("test-kafka");
        if (!CollectionUtils.isEmpty(partitionInfos)) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
            STANDALONE_CONSUMER.assign(partitions);
            while (true) {
                ConsumerRecords<String, String> poll = STANDALONE_CONSUMER.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : poll) {
                    log.info("message:{}", record);
                }
                STANDALONE_CONSUMER.commitSync();
            }
        }
    }
}
