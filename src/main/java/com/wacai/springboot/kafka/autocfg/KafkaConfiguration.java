package com.wacai.springboot.kafka.autocfg;

import com.wacai.springboot.kafka.component.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
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


    /**
     * 底层的 {@link Producer} 实例, 仅仅是纯粹的发消息.
     */
    @Bean
    Producer<String, String> producer(Kafka kafka) throws IOException {
        return new KafkaProducer<>(kafka.getProps(), new StringSerializer(), new StringSerializer());
    }

    /**
     * 用于重新发送之前 {@link KafkaConfiguration#recoverableProducer(Kafka, String, int)} 发送失败且存储在本地的消息
     *
     * @see KafkaConfiguration#producer(Kafka)
     */
    @Bean
    @ConditionalOnProperty("springboot.kafka.recovery.dir")
    Recovery recovery(Kafka kafka, @Value("${springboot.kafka.recovery.dir}") String dir) throws IOException {
        return new Recovery(new File(dir), producer(kafka));
    }

    /**
     * 消息是有可能因为网络等原因而发送失败的, 此 {@link Producer} 实例会将发送失败的消息暂存到本地目录中.
     * <p/>
     * 待需要重发时, 可以使用{@link KafkaConfiguration#recovery(Kafka, String)}来恢复.
     *
     * @see KafkaConfiguration#producer(Kafka)
     */
    @Bean
    @ConditionalOnProperty("springboot.kafka.recovery.dir")
    Producer<String, String> recoverableProducer(
            Kafka kafka,
            @Value("${springboot.kafka.recovery.dir}") String dir,
            @Value("${springboot.kafka.recovery.backlog:128}") int backlog
    ) throws IOException {
        final File directory = new File(dir);
        if (!directory.exists()) directory.mkdirs();
        return new RecoverableProducer(producer(kafka), directory, backlog);
    }

    /**
     * @see RandomTopics
     */
    @Bean
    @ConditionalOnProperty(prefix = "springboot.kafka.topic", name = {"prefix", "parallel"})
    RandomTopics randomTopics(
            @Value("${springboot.kafka.topic.prefix}") String prefix,
            @Value("${springboot.kafka.topic.parallel}") int parallel
    ) {
        return new RandomTopics(prefix, parallel);
    }

    /**
     * @see ModTopics
     */
    @Bean
    @ConditionalOnProperty(prefix = "springboot.kafka.topic", name = {"prefix", "parallel"})
    ModTopics modTopics(
            @Value("${springboot.kafka.topic.prefix}") String prefix,
            @Value("${springboot.kafka.topic.parallel}") int parallel
    ) {
        return new ModTopics(prefix, parallel);
    }

    /**
     * @see MetadataPreloader
     */
    @Bean
    MetadataPreloader metadataPreloader(
            @Value("${springboot.kafka.metadata.timeout:500}") long timeoutMillis,
            @Value("${springboot.kafka.metadata.ignore:ignore}") String ignoreMessage,
            Kafka kafka) throws IOException {
        return new MetadataPreloader(timeoutMillis, ignoreMessage, producer(kafka));
    }

}
