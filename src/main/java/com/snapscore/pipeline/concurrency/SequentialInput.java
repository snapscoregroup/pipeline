package com.snapscore.pipeline.concurrency;

public class SequentialInput<I, R> {

    final I input;
    final QueueResolver<I> queueResolver;
    final SequentialFluxSubscriber<I, R> sequentialFluxSubscriber;
    final LoggingInfo loggingInfo;

    public SequentialInput(I input,
                           QueueResolver<I> queueResolver,
                           SequentialFluxSubscriber<I, R> sequentialFluxSubscriber,
                           LoggingInfo loggingInfo) {
        this.input = input;
        this.queueResolver = queueResolver;
        this.sequentialFluxSubscriber = sequentialFluxSubscriber;
        this.loggingInfo = loggingInfo;
    }

}
