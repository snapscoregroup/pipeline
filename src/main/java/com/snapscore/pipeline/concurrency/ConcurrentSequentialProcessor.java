package com.snapscore.pipeline.concurrency;

import java.time.Duration;

/**
 * Makes it possible to process any data in full parallel mode but also preserving order defined
 * by each {@link SequentialInput}
 */
public interface ConcurrentSequentialProcessor {

    <I, R> void processSequentiallyAsync(SequentialInput<I, R> sequentialInput);

    void awaitProcessingCompletion(Duration timeout) throws Exception;

    long getTotalUnprocessedInputs();
}
