package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class InputProcessingFutureRunner<I, R> extends InputProcessingRunner<I, R> {

    private final static Logger logger = Logger.setup(InputProcessingFutureRunner.class);

    /**
     * This scheduler will be used to subscribe the flux create from the specific completableFuture;
     * Internally the completableFuture will still run on any provided Executors (Flux respects these and does not override them with this the scheduler below)
     */
    private static final Scheduler subscribeOnScheduler = Schedulers.newBoundedElastic(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE, "seq-processing-subscription-thread");

    private final InputProcessingFluxRunner<I, R> inputProcessingFluxRunner;

    public InputProcessingFutureRunner(I input,
                                       CompletableFuture<R> inputProcessing,
                                       LoggingInfo loggingInfo) {
        final Function<I, Flux<R>> processingFluxCreator = i -> {
            return Mono.fromFuture(inputProcessing).flux();
        };

        final Consumer<? super R> subscribeConsumer = result -> {
            if (loggingInfo.logActivity) {
                loggingInfo.decorate(logger).info("Finished processing input {}", loggingInfo.inputDescription);
            }
        };

        final Consumer<? super Throwable> subscribeErrorConsumer = error -> {
            if (loggingInfo.logActivity) {
                loggingInfo.decorate(logger).error("Error processing input {}", loggingInfo.inputDescription, error);
            }
        };

        this.inputProcessingFluxRunner = new InputProcessingFluxRunner<>(
                input,
                processingFluxCreator,
                subscribeConsumer,
                subscribeErrorConsumer,
                loggingInfo,
                subscribeOnScheduler
        );
    }

    @Override
    protected void run(Runnable onTerminateHook, Runnable onCancelHook, long itemEnqueuedTs) {
        inputProcessingFluxRunner.run(onTerminateHook, onCancelHook, itemEnqueuedTs);
    }

}
