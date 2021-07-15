package com.snapscore.pipeline.concurrency;

import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Collections;
import java.util.Set;

/**
 * When we have multiple {@link ConcurrentSequentialProcessor} that execute code that can in turn call another {@link ConcurrentSequentialProcessor}
 * This registry can be used to keep track of all existing {@link ConcurrentSequentialProcessor} and run operations on top of them
 * using {@link ConcurrentSequentialProcessorCompletionAwaiter}
 */
public class ConcurrentSequentialProcessorRegistry {

    private final ConcurrentHashSet<ConcurrentSequentialProcessor> registeredProcessors = new ConcurrentHashSet<>();

    public ConcurrentSequentialProcessorRegistry() {
    }

    public void register(ConcurrentSequentialProcessor processor) {
        this.registeredProcessors.add(processor);
    }

    public Set<ConcurrentSequentialProcessor> getRegisteredProcessors() {
        return Collections.unmodifiableSet(registeredProcessors);
    }

}
