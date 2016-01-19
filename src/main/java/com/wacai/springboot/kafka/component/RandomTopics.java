package com.wacai.springboot.kafka.component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** 大量消息产生和消费的场景中, 可以采用多 topic 的方式增加并行度. 此类可提供了 Topic 发送的随即均衡策略. */
public class RandomTopics {
    private final String prefix;
    private final int    parallel;

    public RandomTopics(String prefix, int parallel) {
        this.prefix = prefix;
        this.parallel = parallel;
    }

    public Stream<String> all() {
        return IntStream.range(0, parallel).mapToObj(i -> prefix + i);
    }

    public String random() {
        return prefix + ThreadLocalRandom.current().nextInt(parallel);
    }
}
