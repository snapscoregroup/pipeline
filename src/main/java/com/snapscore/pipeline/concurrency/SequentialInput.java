package com.snapscore.pipeline.concurrency;

public class SequentialInput<I, R> {

    final I input;
    final InputQueueResolver<I> inputQueueResolver;
    final InputProcessingRunner<I, R> inputProcessingRunner;
    final LoggingInfo loggingInfo;

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
