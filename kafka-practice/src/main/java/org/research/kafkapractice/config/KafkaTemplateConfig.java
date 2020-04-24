package org.research.kafkapractice.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * @fileName: KafkaTemplateConfig.java
 * @description: KafkaTemplateConfig.java类说明
 * @author: by echo huang
 * @date: 2020-04-22 10:53
 */
@Configuration
public class KafkaTemplateConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> stringObjectMap = kafkaProperties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(stringObjectMap);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
