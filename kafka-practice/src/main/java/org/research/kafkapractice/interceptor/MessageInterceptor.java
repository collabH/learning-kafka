package org.research.kafkapractice.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @fileName: MessageInterceptor.java
 * @description: 消息前置拦截器
 * @author: by echo huang
 * @date: 2020-05-10 15:42
 */
public class MessageInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        record = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(),
                record.value() + "interceptor", record.headers());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null && "test".equals(metadata.topic())
                && metadata.partition() == 0) {
            // 对于正常返回，Topic为test且分区编号为0的消息的返回信息进行输出
            System.out.println(metadata.toString());
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
