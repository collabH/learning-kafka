package org.research.kafkapractice.consumer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.research.kafkapractice.consumer.OriginalConsumer.CONCURRENT_CONUSMER;
import static org.research.kafkapractice.consumer.OriginalConsumer.KAFKA_CONSUMER;

/**
 * @fileName: ShutDownHookConsumer.java
 * @description: ShutDownHookConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-04-27 15:17
 */
@Slf4j
@Component
public class ShutDownHookConsumer implements DisposableBean {

    private static final AtomicInteger CONSUMER_COUNTER = new AtomicInteger(0);

    public void shutDownConsumer() {
        KAFKA_CONSUMER.subscribe(Lists.newArrayList("learning-kafka"));
        try {
            while (true) {
                CONCURRENT_CONUSMER.submit(() -> {
                    ConsumerRecords<String, String> poll = KAFKA_CONSUMER.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> stringStringConsumerRecord : poll) {
                        log.info("message:{}", stringStringConsumerRecord);
                        CONSUMER_COUNTER.incrementAndGet();
                    }
                    if (CONSUMER_COUNTER.compareAndSet(10, 0)) {
                        shutdownHook();
                    }
                    KAFKA_CONSUMER.commitAsync();
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            KAFKA_CONSUMER.close();
        }
    }

    public void shutdownHook() {
        log.info("shutdown");
        Runtime.getRuntime().addShutdownHook(new Thread(KAFKA_CONSUMER::wakeup));
    }

    @Override
    public void destroy() throws Exception {

    }
}
