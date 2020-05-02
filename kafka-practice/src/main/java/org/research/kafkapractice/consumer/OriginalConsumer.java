package org.research.kafkapractice.consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.research.kafkapractice.rebalance.HandleRebalance;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @fileName: OriginalConsumer.java
 * @description: 原始API消费者
 * @author: by echo huang
 * @date: 2020-04-26 11:23
 */
@Component
@Slf4j
public class OriginalConsumer implements DisposableBean {

    public static final KafkaConsumer<String, String> KAFKA_CONSUMER;
    public static final ExecutorService CONCURRENT_CONUSMER;

    static {
        KAFKA_CONSUMER = new KafkaConsumer<>(createConsumerProperties());
        CONCURRENT_CONUSMER = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() / 2, Runtime.getRuntime().availableProcessors(),
                3, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(1000),
                new ThreadFactoryBuilder()
                        .setNameFormat("original-consumer-%d")
                        .build(), new ThreadPoolExecutor.CallerRunsPolicy());
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
        return properties;
    }

    public void consumer() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"));

        while (true) {
            ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            if (iterator.hasNext()) {
                log.info("message:{}", iterator.next());
            }
            try {
                KAFKA_CONSUMER.commitSync();
            } catch (Exception e) {
                log.error("提交offset失败");
            }


        }
    }

    /**
     * 异步提交offset
     */
    public void asyncCommitOffsetConsumer() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"));
        while (true) {
            ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            if (iterator.hasNext()) {
                log.info("message:{}", iterator.next());
            }
         /*   try {
                KAFKA_CONSUMER.commitAsync();
            } catch (Exception e) {
                log.error("提交offset失败");
            }*/
            //使用回调
            try {
                KAFKA_CONSUMER.commitAsync((offsets, exception) -> {
                    if (Objects.nonNull(exception)) {
                        log.error("commit fail for offsets:{}", offsets, exception);
                    }
                });

            } catch (Exception e) {
                log.error("offset提交异常");
            }
        }
    }

    /**
     * 同步和异步提交组合
     */
    public void combinationCommitOffset() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"));
        try {
            while (true) {
                ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
                Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
                if (iterator.hasNext()) {
                    log.info("message:{}", iterator.next());
                }

                //使用回调
                KAFKA_CONSUMER.commitAsync((offsets, exception) -> {
                    if (Objects.nonNull(exception)) {
                        log.error("commit fail for offsets:{}", offsets, exception);
                    }
                });
            }
        } catch (Exception e) {
            log.error("offset提交异常");
        } finally {
            try {
                KAFKA_CONSUMER.commitSync();
            } catch (Exception e) {
                log.error("同步提交一次");
            } finally {
                KAFKA_CONSUMER.close();
            }

        }
    }

    private Map<TopicPartition, OffsetAndMetadata> OFFSET = Maps.newHashMap();

    int count = 0;

    /**
     * 提交任意偏移量
     */
    public void commitArbitrarilyOffset() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"));
        try {
            while (true) {
                ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
                Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
                if (iterator.hasNext()) {
                    ConsumerRecord<String, String> next = iterator.next();
                    log.info("message:{}", next);
                    OFFSET.put(new TopicPartition(next.topic(), next.partition()), new OffsetAndMetadata(next.offset() + 1, "no metadata"));
                    if (count % 1000 == 0) {
                        KAFKA_CONSUMER.commitSync(OFFSET, null);
                    }
                    count++;
                }


            }
        } catch (Exception e) {
            log.error("offset提交异常");
        }
    }


    /**
     * 并发消费者
     */
    public void concurrentConsumer() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"));
        while (true) {
            CONCURRENT_CONUSMER.submit(() -> {
                ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
                Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
                if (iterator.hasNext()) {
                    log.info("message:{}", iterator.next());
                }
            });
        }
    }

    /**
     * 使用再平衡监听器
     */
    public void reblaceListenerConsumer() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"), new HandleRebalance(KAFKA_CONSUMER, OFFSET));

        try {
            while (true) {
                ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
                Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
                if (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    OFFSET.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    KAFKA_CONSUMER.commitSync(OFFSET);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            KAFKA_CONSUMER.close();
        }

    }

    @Override
    public void destroy() throws Exception {
        //直接关闭消费者，直接触发一次再均衡
        KAFKA_CONSUMER.close();
    }
}
