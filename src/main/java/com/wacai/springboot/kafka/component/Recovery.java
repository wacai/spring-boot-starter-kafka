package com.wacai.springboot.kafka.component;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

public class Recovery implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Recovery.class);

    private final File dir;

    private final Producer<String, String> producer;

    public Recovery(File dir, Producer<String, String> producer) {
        this.dir = dir;
        this.producer = producer;
    }

    @Override
    public void run() {
        final File[] files = dir.listFiles();
        if (files == null) return;

        Stream.of(files).parallel().forEach(f -> {

            try {
                 Files.lines(f.toPath())
                      .forEach(value -> producer.send(new ProducerRecord<>(f.getName(), value)));
            } catch (IOException e) {
                LOGGER.error("Failed to recover {}", f, e);
            }
        });
    }
}
