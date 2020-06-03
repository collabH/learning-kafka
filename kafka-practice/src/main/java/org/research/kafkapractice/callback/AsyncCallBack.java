package org.research.kafkapractice.callback;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @fileName: AsyncCallBack.java
 * @description: AsyncCallBack.java类说明
 * @author: by echo huang
 * @date: 2020-05-06 17:02
 */
@Slf4j
public class AsyncCallBack implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        log.info("metadata:{}", metadata);
    }
}
