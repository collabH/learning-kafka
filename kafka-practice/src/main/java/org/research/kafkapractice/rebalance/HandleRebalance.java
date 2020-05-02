package org.research.kafkapractice.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * @fileName: HandleRebalance.java
 * @description: 监听再均衡
 * @author: by echo huang
 * @date: 2020-04-26 23:39
 */
@Slf4j
public class HandleRebalance implements ConsumerRebalanceListener {

    private KafkaConsumer<String, String> consumer;

    private Map<TopicPartition, OffsetAndMetadata> currentOffset;

    public HandleRebalance(KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffset) {
        this.consumer = consumer;
        this.currentOffset = currentOffset;
    }

    /**
     * 再均衡开始之前和消费者停止读取消息之后被调用。如果这里提交offset，下一个接管分区的consumer就知道从哪里开始消费了
     *
     * @param partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("再均衡之前，consumer停止消费后提交offset给服务器");
        consumer.commitSync(currentOffset);
    }

    /**
     * 在重新分配分区之后和消费者开始读取消息之前被调用
     *
     * @param partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info(" 在重新分配分区之后和消费者开始读取消息之前被调用");
    }

}
