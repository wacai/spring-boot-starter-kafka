package com.wacai.springboot.kafka.autocfg;

import com.wacai.springboot.kafka.component.MetadataPreloader;
import com.wacai.springboot.kafka.component.RandomTopics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean
    KafkaProducer<String, String> producer(@Value("${springboot.kafka.config:./kafka.properties}") String props)
            throws IOException {
        return new KafkaProducer<>(config(new File(props)), new StringSerializer(), new StringSerializer());
    }

    @Bean
    @ConditionalOnProperty(prefix = "springboot.kafka.topic")
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
            KafkaProducer<String, String> producer) {
        return new MetadataPreloader(timeoutMillis, ignoreMessage, producer);
    }


    private Properties config(File file) throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            props.load(in);
        }
        return props;
    }

}
