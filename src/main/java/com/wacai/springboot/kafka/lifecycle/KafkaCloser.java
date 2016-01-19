package com.wacai.springboot.kafka.lifecycle;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

@Component
public class KafkaCloser implements ApplicationListener<ContextClosedEvent> {

    @Autowired
    KafkaProducer<String, String> kafkaProducer;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        kafkaProducer.close();
    }
}
