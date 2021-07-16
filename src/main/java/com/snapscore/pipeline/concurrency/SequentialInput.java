package com.snapscore.pipeline.concurrency;

/**
 * Holds data that will be processed in parallel but also in a strictly sequential order defined by the specified
 * {@link InputQueueResolver}
 */
public class SequentialInput<I, R> {

    final I input;
    final InputQueueResolver<I> inputQueueResolver;
    final InputProcessingRunner<I, R> inputProcessingRunner;
    final LoggingInfo loggingInfo;

    /**
     * @param inputQueueResolver defines the ordering rules in which a piece of data will be processed
     */
    public SequentialInput(I input,
                           InputQueueResolver<I> inputQueueResolver,
                           InputProcessingRunner<I, R> inputProcessingRunner,
                           LoggingInfo loggingInfo) {
        this.input = input;
        this.inputQueueResolver = inputQueueResolver;
        this.inputProcessingRunner = inputProcessingRunner;
        this.loggingInfo = loggingInfo;
    }

}
