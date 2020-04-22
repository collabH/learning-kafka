package org.research.kafkapractice.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @fileName: KafkaConsumer.java
 * @description: KafkaConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-04-22 11:17
 */
@Component
public class KafkaConsumer {


    @KafkaListener(topics = "test-topic", concurrency = "4")
    public void recv(String data) {
        System.out.println(data);
    }
}
