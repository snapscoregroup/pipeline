package com.snapscore.pipeline.concurrency;

import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Collections;
import java.util.Set;

public class ConcurrentSequentialProcessorRegistry {

    private final ConcurrentHashSet<ConcurrentSequentialProcessor> registeredProcessors = new ConcurrentHashSet<>();

    public ConcurrentSequentialProcessorRegistry() {
    }

    public void register(ConcurrentSequentialProcessor concurrentSequentialProcessor) {
        this.registeredProcessors.add(concurrentSequentialProcessor);
    }

    public Set<ConcurrentSequentialProcessor> getRegisteredProcessors() {
        return Collections.unmodifiableSet(registeredProcessors);
    }

}
