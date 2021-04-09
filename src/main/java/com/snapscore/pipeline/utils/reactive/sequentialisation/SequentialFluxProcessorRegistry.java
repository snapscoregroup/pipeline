package com.snapscore.pipeline.utils.reactive.sequentialisation;

import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SequentialFluxProcessorRegistry {

    private final ConcurrentHashSet<SequentialFluxProcessor> registeredProcessors = new ConcurrentHashSet<>();

    public SequentialFluxProcessorRegistry() {
    }

    public void register(SequentialFluxProcessor sequentialFluxProcessor) {
        this.registeredProcessors.add(sequentialFluxProcessor);
    }

    public Set<SequentialFluxProcessor> getRegisteredProcessors() {
        return Collections.unmodifiableSet(registeredProcessors);
    }

}
