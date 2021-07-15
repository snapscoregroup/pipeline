package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class SequentialFluxProcessorCompletionAwaiter {

    private final static Logger logger = Logger.setup(SequentialFluxProcessorCompletionAwaiter.class);

    public static final int MAX_ITERATIONS_COUNT = 10_000;

    public SequentialFluxProcessorCompletionAwaiter() {
    }

    /**
     *
     * @param sequentialFluxProcessorSet a set of processors whose processing might be potentially interdependent
     *                                   in the sense that the processing of one processor generates tasks for another processor
     *                                   (and vice versa). As a result multiple repeated checks for completion are necessary.
     */
    public static void awaitProcessingCompletionOf(Set<SequentialFluxProcessor> sequentialFluxProcessorSet, Duration timeout) throws Exception {

        final long timeoutMillis = timeout.toMillis();
        final long start = System.currentTimeMillis();

        for (int count = 0; count < MAX_ITERATIONS_COUNT; count++) {

            logger.info("awaiting completion of processing; Iteration no. {}", count);

            boolean anyUnprocessedInputs = false;

            for (SequentialFluxProcessor sequentialFluxProcessor : sequentialFluxProcessorSet) {
                long nextTimeoutMillis = timeoutMillis - (System.currentTimeMillis() - start);
                if (nextTimeoutMillis < 0L) {
                    throw new TimeoutException("Timeout waiting for sequentialFluxProcessor to complete processing");
                }

                if (sequentialFluxProcessor.getTotalUnprocessedInputs() > 0L) {
                    anyUnprocessedInputs = true;
                    sequentialFluxProcessor.awaitProcessingCompletion(Duration.ofMillis(nextTimeoutMillis));
                }
            }

            if (!anyUnprocessedInputs) {
                break;
            }

        }

    }

}
