package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class SequentialFluxSubscriberNonReactive<I, R> implements SequentialFluxSubscriberInterface {

    private final static Logger logger = Logger.setup(SequentialFluxSubscriberNonReactive.class);

    /**
     * This scheduler will be used to subscribe the flux create from the specific completableFuture;
     * Internally the completableFuture will still run on any provided Executors (Flux respects these and does not override them with this the scheduler below)
     */
    private static final Scheduler subscribeOnScheduler = Schedulers.newBoundedElastic(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE, "seq-processing-subscription-thread");

    private final SequentialFluxSubscriber<I, R> sequentialFluxSubscriber;

    public SequentialFluxSubscriberNonReactive(I input,
                                               CompletableFuture<R> inputProcessing,
                                               LoggingInfo loggingInfo) {
        final Function<I, Flux<R>> processingFluxCreator = i -> {
            return Mono.fromFuture(inputProcessing).flux();
        };

        final Consumer<? super R> subscribeConsumer = result -> {if (loggingInfo.logActivity) {
            loggingInfo.decorate(logger).info("Got operation result ...");
        }};

        final Consumer<? super Throwable> subscribeErrorConsumer = error -> {
            if (loggingInfo.logActivity) {
                loggingInfo.decorate(logger).info("Got operation error ...");
            }
        };

        this.sequentialFluxSubscriber = new SequentialFluxSubscriber<>(
                input,
                processingFluxCreator,
                subscribeConsumer,
                subscribeErrorConsumer,
                loggingInfo,
                subscribeOnScheduler
        );
    }

    @Override
    public void subscribe(Runnable onTerminateHook, Runnable onCancelHook, long itemEnqueuedTs) {
        sequentialFluxSubscriber.subscribe(onTerminateHook, onCancelHook, itemEnqueuedTs);
    }

}
