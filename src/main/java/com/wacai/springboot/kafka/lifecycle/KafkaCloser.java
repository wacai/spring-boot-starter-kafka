package com.wacai.springboot.kafka.lifecycle;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaCloser implements ApplicationListener<ContextClosedEvent> {

    @Autowired(required = false)
    Map<String, Producer<?,?>> producers;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        producers.values().forEach(Producer::close);
    }
}
