package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Makes it possible to awayt the (possibly inter-dependent) processing of specific Set of {@link ConcurrentSequentialProcessor}s
 */
public class ConcurrentSequentialProcessorCompletionAwaiter {

    public static final int MAX_ITERATIONS_COUNT = 10_000;
    private final static Logger logger = Logger.setup(ConcurrentSequentialProcessorCompletionAwaiter.class);

    public ConcurrentSequentialProcessorCompletionAwaiter() {
    }

    /**
     * @param processors a set of processors whose processing might be potentially interdependent
     *                                         in the sense that the processing of one processor generates tasks for another processor
     *                                         (and vice versa). As a result multiple repeated checks for completion are necessary.
     */
    public static void awaitProcessingCompletionOf(Set<ConcurrentSequentialProcessor> processors, Duration timeout) throws Exception {

        final long timeoutMillis = timeout.toMillis();
        final long start = System.currentTimeMillis();

        for (int count = 0; count < MAX_ITERATIONS_COUNT; count++) {

            logger.info("awaiting completion of processing; Iteration no. {}", count);

            boolean anyUnprocessedInputs = false;

            for (ConcurrentSequentialProcessor concurrentSequentialProcessor : processors) {
                long nextTimeoutMillis = timeoutMillis - (System.currentTimeMillis() - start);
                if (nextTimeoutMillis < 0L) {
                    throw new TimeoutException("Timeout waiting for concurrentSequentialProcessor to complete processing");
                }

                if (concurrentSequentialProcessor.getTotalUnprocessedInputs() > 0L) {
                    anyUnprocessedInputs = true;
                    concurrentSequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(nextTimeoutMillis));
                }
            }

            if (!anyUnprocessedInputs) {
                break;
            }

        }

    }

}
