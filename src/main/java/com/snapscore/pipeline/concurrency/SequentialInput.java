package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

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
     * DEPRECATED use an appropriate newBuilder instead
     *
     * @param inputQueueResolver defines the ordering rules in which a piece of data will be processed
     */
    @Deprecated
    public SequentialInput(I input,
                           InputQueueResolver<I> inputQueueResolver,
                           InputProcessingRunner<I, R> inputProcessingRunner,
                           LoggingInfo loggingInfo) {
        this.input = input;
        this.inputQueueResolver = inputQueueResolver;
        this.inputProcessingRunner = inputProcessingRunner;
        this.loggingInfo = loggingInfo;
    }

    public static <I, R> Builder<I, R> newBuilder(I input, InputQueueResolver<I> inputQueueResolver, Function<I, Flux<R>> processingFluxCreator) {
        return new Builder<>(input, inputQueueResolver, processingFluxCreator);
    }

    public static <I, R> Builder<I, R> newBuilder(I input, InputQueueResolver<I> inputQueueResolver, Callable<R> inputProcessing) {
        return new Builder<>(input, inputQueueResolver, inputProcessing);
    }


    public static class Builder<I, R> {

        private final I input;
        private final InputQueueResolver<I> inputQueueResolver;
        private Function<I, Flux<R>> processingFluxCreator;
        private Callable<R> inputProcessing;
        private Consumer<? super R> subscribeConsumer;
        private Consumer<? super Throwable> subscribeErrorConsumer;
        private final LoggingInfo.Builder loggingInfoBuilder = LoggingInfo.builder();
        private Scheduler subscribeOnScheduler;

        public Builder(I input, InputQueueResolver<I> inputQueueResolver, Function<I, Flux<R>> processingFluxCreator) {
            this.input = input;
            this.inputQueueResolver = inputQueueResolver;
            this.processingFluxCreator = processingFluxCreator;
        }

        public Builder(I input, InputQueueResolver<I> inputQueueResolver, Callable<R> inputProcessing) {
            this.input = input;
            this.inputQueueResolver = inputQueueResolver;
            this.inputProcessing = inputProcessing;
        }

        public Builder<I, R> setSubscribeConsumer(Consumer<? super R> subscribeConsumer) {
            this.subscribeConsumer = subscribeConsumer;
            return this;
        }

        public Builder<I, R> setSubscribeErrorConsumer(Consumer<? super Throwable> subscribeErrorConsumer) {
            this.subscribeErrorConsumer = subscribeErrorConsumer;
            return this;
        }

        public Builder<I, R> setSubscribeOnScheduler(Scheduler subscribeOnScheduler) {
            this.subscribeOnScheduler = subscribeOnScheduler;
            return this;
        }

        public Builder<I, R> setLogActivity(boolean logActivity) {
            this.loggingInfoBuilder.setLogActivity(logActivity);
            return this;
        }

        public Builder<I, R> setInputDescription(String inputDescription) {
            this.loggingInfoBuilder.setInputDescription(inputDescription);
            this.setLogActivity(true);
            return this;
        }

        public Builder<I, R> setLoggerDecorator(Function<Logger, Logger> loggerDecorator) {
            this.loggingInfoBuilder.setLoggerDecorator(loggerDecorator);
            this.setLogActivity(true);
            return this;
        }

        public SequentialInput<I, R> build() {
            final InputProcessingRunner<I, R> inputProcessingRunner;
            final LoggingInfo loggingInfo = this.loggingInfoBuilder.build();
            if (this.processingFluxCreator != null) {
                inputProcessingRunner = new InputProcessingFluxRunner<>(this.input, this.processingFluxCreator, this.subscribeConsumer, this.subscribeErrorConsumer, loggingInfo, this.subscribeOnScheduler);
            } else if (this.inputProcessing != null) {
                inputProcessingRunner = new InputProcessingCallableRunner<>(this.input, this.inputProcessing, loggingInfo);
            } else {
                throw new IllegalStateException("Cannot set inputProcessingRunner!");
            }

            return new SequentialInput<>(this.input, this.inputQueueResolver, inputProcessingRunner, loggingInfo);
        }

    }

}

