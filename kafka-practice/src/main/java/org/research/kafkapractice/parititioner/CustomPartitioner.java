package org.research.kafkapractice.parititioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @fileName: CustomPartitioner.java
 * @description: 自定义分区器
 * @author: by echo huang
 * @date: 2020-04-24 17:37
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //拿到全部的分区
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        //分区的总数
        int numPartitions = partitionInfos.size();
        if (Objects.isNull(keyBytes) || !(key instanceof String)) {
            throw new InvalidRecordException("we expect all messages to have consumer name as key");
        }
        //如果key为Last让其写入最后一个分区中
        if ("Last".equals(String.valueOf(key))) {
            return numPartitions - 1;
        }
        //其他散列到其他分区
        return Math.abs(Utils.murmur2(keyBytes) % (numPartitions - 2));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
