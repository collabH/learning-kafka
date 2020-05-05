package org.research.kafkapractice.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @fileName: KafkaConsumer.java
 * @description: KafkaConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-04-22 11:17
 */
//@Component
@Slf4j
public class KafkaConsumer implements KafkaListenerErrorHandler {

    private static final AtomicBoolean INIT_CONSUMER = new AtomicBoolean(true);


    @KafkaListener(topics = "test-topic", concurrency = "4")
    public void recv(ConsumerRecord<String, String> data, Consumer<String, String> consumer) {
//        Map<TopicPartition, Long> topicPartitionTimeStampMap = new HashMap<>();
//
//        TopicPartition partition0 = new TopicPartition(data.topic(), data.partition());
//
//        topicPartitionTimeStampMap.put(partition0, 1587639336760L);
//        //拿到该时间戳的offset
//        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(topicPartitionTimeStampMap);
//
//        long offset = offsetAndTimestampMap.get(partition0).offset();
//        consumer.seek(partition0, offset);
//
        log.info(data.value());
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        return null;
    }
}
