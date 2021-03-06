package org.research.kafkapractice.producer;

import org.research.kafkapractice.consumer.OriginalConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @fileName: MultiProducer.java
 * @description: MultiProducer.java类说明
 * @author: by echo huang
 * @date: 2020-04-22 10:35
 */
@Component
public class MultiProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void send1() throws InterruptedException {
        while (true) {
            OriginalConsumer.CONCURRENT_CONUSMER.submit(() -> {
                kafkaTemplate.send("test-topic", "hello world1");
            });
            Thread.sleep(100);
        }
    }

    public void send2() {
        kafkaTemplate.send("test-topic", "hello world2");
    }

    public void send3() {
        kafkaTemplate.send("test-topic", "hello world3");
    }
}
