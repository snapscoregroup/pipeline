package com.snapscore.pipeline.utils.reactive.sequentialisation;

import java.time.Duration;

public interface SequentialFluxProcessor {

    <I, R> void processSequentiallyAsync(SequentialInput<I, R> sequentialInput);

    void awaitProcessingCompletion(Duration timeout) throws Exception;

    long getTotalUnprocessedInputs();
}
