package com.snapscore.pipeline.concurrency;

import java.time.Duration;

public interface ConcurrentSequentialProcessor {

    <I, R> void processSequentiallyAsync(SequentialInput<I, R> sequentialInput);

    void awaitProcessingCompletion(Duration timeout) throws Exception;

    long getTotalUnprocessedInputs();
}
