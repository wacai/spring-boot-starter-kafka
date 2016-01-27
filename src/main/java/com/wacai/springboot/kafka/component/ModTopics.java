package com.wacai.springboot.kafka.component;

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** 基于取模策略的 Topic 选择 */
public class ModTopics {
    private final String prefix;
    private final int    parallel;

    public ModTopics(String prefix, int parallel) {
        this.prefix = prefix;
        this.parallel = parallel;
    }

    public Stream<String> all() {
        return IntStream.range(0, parallel).mapToObj(i -> prefix + i);
    }

    public String topic(Function<Integer, Integer> mod) {
        return prefix + mod.apply(parallel);
    }
}
