package com.wacai.springboot.kafka.autocfg;

import com.wacai.springboot.kafka.component.MetadataPreloader;
import com.wacai.springboot.kafka.component.RandomTopics;
import com.wacai.springboot.kafka.component.RecoverableProducer;
import com.wacai.springboot.kafka.component.Recovery;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Bean
    Producer<String, String> producer(@Qualifier("kafkaConfig") Map<String, Object> props)
            throws IOException {
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @Bean
    @ConditionalOnProperty("springboot.kafka.recovery")
    Recovery recovery(
            @Qualifier("producer") Producer<String, String> producer,
            @Value("${springboot.kafka.recovery.dir}") String dir
    ) {
        return new Recovery(new File(dir), producer);
    }

    @Bean
    @ConditionalOnProperty("springboot.kafka.recovery")
    Producer<String, String> recoverableProducer(
            @Qualifier("producer") Producer<String, String> producer,
            @Value("${springboot.kafka.recovery.dir}") String dir,
            @Value("${springboot.kafka.recovery.backlog:128}") int backlog) {
        return new RecoverableProducer(producer, new File(dir), backlog);
    }

    @Bean
    @ConditionalOnProperty("springboot.kafka.topic")
    RandomTopics randomTopics(
            @Value("${springboot.kafka.topic.prefix}") String prefix,
            @Value("${springboot.kafka.topic.parallel}") int parallel
    ) {
        return new RandomTopics(prefix, parallel);
    }

    @Bean
    MetadataPreloader metadataPreloader(
            @Value("${springboot.kafka.metadata.timeout:500}") long timeoutMillis,
            @Value("${springboot.kafka.metadata.ignore:ignore}") String ignoreMessage,
            @Qualifier("producer") Producer<String, String> producer) {
        return new MetadataPreloader(timeoutMillis, ignoreMessage, producer);
    }

}
