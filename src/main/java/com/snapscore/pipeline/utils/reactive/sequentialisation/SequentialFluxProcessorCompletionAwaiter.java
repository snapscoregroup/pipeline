package com.snapscore.pipeline.utils.reactive.sequentialisation;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class SequentialFluxProcessorCompletionAwaiter {

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

        while (true) {

            boolean anyUnprocessedInputs = false;

            for (SequentialFluxProcessor sequentialFluxProcessor : sequentialFluxProcessorSet) {
                long nextTimeoutMillis = timeoutMillis - (System.currentTimeMillis() - start);
                if (nextTimeoutMillis < 0L) {
                    throw new TimeoutException("Timeout waiting for sequentialFluxProcessor to complete processing");
                }

                if (sequentialFluxProcessor.getTotalUnprocessedInputs() > 0L) {
                    anyUnprocessedInputs = true;
                    // TODO log how many iterations we did here ...
                    sequentialFluxProcessor.awaitProcessingCompletion(Duration.ofMillis(nextTimeoutMillis));
                }
            }

            if (!anyUnprocessedInputs) {
                break;
            }

        }

    }

}
