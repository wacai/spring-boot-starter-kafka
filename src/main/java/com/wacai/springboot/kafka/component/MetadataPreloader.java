package com.wacai.springboot.kafka.component;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** 预载元数据信息可以快速发现 Kafka 配置(Topic等)错误. */
public class MetadataPreloader {
    private final long                     timeoutMillis;
    private final String                   ignoreMessage;
    private final Producer<String, String> producer;

    public MetadataPreloader(long timeoutMillis, String ignoreMessage, Producer<String, String> producer) {
        this.timeoutMillis = timeoutMillis;
        this.ignoreMessage = ignoreMessage;
        this.producer = producer;
    }

    public void load(Stream<String> topics) throws Exception {
        final List<Future<RecordMetadata>> futures = topics.parallel()
                                                           .map(t -> producer.send(record(t, ignoreMessage)))
                                                           .collect(Collectors.toList());

        for (Future<RecordMetadata> future : futures) {
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private static ProducerRecord<String, String> record(String topic, String message) {
        return new ProducerRecord<>(topic, message);
    }
}
