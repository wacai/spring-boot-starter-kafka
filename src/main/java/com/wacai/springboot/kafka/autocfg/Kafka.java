package com.wacai.springboot.kafka.autocfg;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties("springboot.kafka.conf")
public class Kafka {
    final Map<String, Object> props = new HashMap<>();

    public Map<String, Object> getProps() {
        return props;
    }


}
