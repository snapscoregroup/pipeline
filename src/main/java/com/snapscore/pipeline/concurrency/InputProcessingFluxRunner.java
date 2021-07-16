package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactor Flux-aware implementation that makes it possible to run sync or async logic wrapped inside a Callable
 *  * The logic gets executed asynchronously, in parallel and in a strictly predefined order
 */
public class InputProcessingFluxRunner<I, R> extends InputProcessingRunner<I, R> {

    private final static Logger logger = Logger.setup(InputProcessingFluxRunner.class);

    private final I input;
    private final Function<I, Flux<R>> processingFluxCreator;
    private final Consumer<? super R> subscribeConsumer;
    private final Consumer<? super Throwable> subscribeErrorConsumer;
    private final Scheduler subscribeOnScheduler;
    private final LoggingInfo loggingInfo;

    public InputProcessingFluxRunner(I input,
                                     Function<I, Flux<R>> processingFluxCreator,
                                     Consumer<? super R> subscribeConsumer,
                                     Consumer<? super Throwable> subscribeErrorConsumer,
                                     LoggingInfo loggingInfo,
                                     Scheduler subscribeOnScheduler) {
        this.input = input;
        this.processingFluxCreator = processingFluxCreator;
        this.subscribeConsumer = subscribeConsumer;
        this.subscribeErrorConsumer = subscribeErrorConsumer;
        this.subscribeOnScheduler = subscribeOnScheduler;
        this.loggingInfo = loggingInfo;
    }

    @Override
    protected void run(Runnable onTerminateHook, Runnable onCancelHook, long itemEnqueuedTs) {
        Consumer<? super R> subscribeConsumerWrapped = getSubscribeConsumerWrapped(itemEnqueuedTs);
        processingFluxCreator.apply(input)
                .doOnTerminate(onTerminateHook)
                .doOnCancel(onCancelHook)
                .subscribeOn(subscribeOnScheduler)
                .subscribe(subscribeConsumerWrapped, subscribeErrorConsumer);
    }

    private Consumer<? super R> getSubscribeConsumerWrapped(long itemEnqueuedTs) {
        return result -> {
            subscribeConsumer.accept(result);
            final long end = System.currentTimeMillis();
            final long processingTimeMillis = end - itemEnqueuedTs;
            if (loggingInfo.logActivity) {
                loggingInfo.decorate(logger).decorateSetup(props -> props.analyticsId("input_processing_time").exec(Long.toString(processingTimeMillis))).info("Input took {} ms to process: {}", processingTimeMillis, loggingInfo.inputDescription);
            }
        };
    }


}
