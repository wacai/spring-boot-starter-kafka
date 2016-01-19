package com.wacai.springboot.kafka.component;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class RecoverableProducer implements Producer<String, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverableProducer.class);

    private final Producer<String, String> delegate;
    private final File                     dir;
    private final Saving                   saving;

    public RecoverableProducer(Producer<String, String> delegate, File dir, int backlog) {
        this.delegate = delegate;
        this.dir = dir;
        saving = new Saving(backlog);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
        return send(record, (metadata, exception) -> {});
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
        return delegate.send(record, (metadata, exception) -> {
            if (metadata == null) saving.append(record);
            callback.onCompletion(metadata, exception);
        });
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {return delegate.partitionsFor(topic);}

    @Override
    public Map<MetricName, ? extends Metric> metrics() {return delegate.metrics();}

    @Override
    public void close() {
        saving.running = false;
        delegate.close();
    }

    private class Saving implements Runnable {
        final BlockingQueue<ProducerRecord<String, String>> queue;
        final Map<String, PrintWriter>                      writers;

        volatile boolean running = true;

        private Saving(int backlog) {
            queue = new ArrayBlockingQueue<>(backlog);
            final Thread thread = new Thread(this, "recovery-saving");
            thread.setDaemon(true);
            thread.start();
            writers = new HashMap<>();
        }

        void append(ProducerRecord<String, String> record) {
            queue.offer(record);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    write(queue.poll(500L, TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    running = false;
                    LOGGER.error("Broken recovery saving", e);
                }
            }

            writers.values().forEach(PrintWriter::close);
        }

        private void write(ProducerRecord<String, String> record) {
            if (record == null) return;
            final String topic = record.topic();
            final PrintWriter writer = Optional.ofNullable(writers.get(topic)).orElseGet(() -> writer(topic));
            writer.println(record.value());
        }

        private PrintWriter writer(String topic) {
            try {
                final PrintWriter writer = new PrintWriter(new File(dir, topic));
                writers.put(topic, writer);
                return writer;
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
