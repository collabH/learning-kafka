package org.research.kafkapractice.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @fileName: CustomSerializer.java
 * @description: 自定义序列化器
 * @author: by echo huang
 * @date: 2020-04-24 15:59
 */
public class CustomSerializer implements Serializer<Customer> {

    @Override
    public byte[] serialize(String topic, Customer data) {
        String json = JSON.toJSONString(data);
        return json.getBytes();
    }
}
