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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;

@Configuration
@EnableConfigurationProperties(Kafka.class)
public class KafkaConfiguration {


    @Bean
    Producer<String, String> producer(Kafka kafka)
            throws IOException {
        return new KafkaProducer<>(kafka.getProps(), new StringSerializer(), new StringSerializer());
    }

    @Bean
    @ConditionalOnProperty("springboot.kafka.recovery.dir")
    Recovery recovery(
            @Qualifier("producer") Producer<String, String> producer,
            @Value("${springboot.kafka.recovery.dir}") String dir
    ) {
        return new Recovery(new File(dir), producer);
    }

    @Bean
    @ConditionalOnProperty("springboot.kafka.recovery.dir")
    Producer<String, String> recoverableProducer(
            @Qualifier("producer") Producer<String, String> producer,
            @Value("${springboot.kafka.recovery.dir}") String dir,
            @Value("${springboot.kafka.recovery.backlog:128}") int backlog) {
        final File directory = new File(dir);
        if (!directory.exists()) directory.mkdirs();
        return new RecoverableProducer(producer, directory, backlog);
    }

    @Bean
    @ConditionalOnProperty(prefix = "springboot.kafka.topic", name = {"prefix", "parallel"})
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
