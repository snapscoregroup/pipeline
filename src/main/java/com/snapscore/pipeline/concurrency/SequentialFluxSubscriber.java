package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

public class SequentialFluxSubscriber<I, R> implements SequentialFluxSubscriberInterface {

    private final static Logger logger = Logger.setup(SequentialFluxSubscriber.class);

    private final I input;
    private final Function<I, Flux<R>> processingFluxCreator;
    private final Consumer<? super R> subscribeConsumer;
    private final Consumer<? super Throwable> subscribeErrorConsumer;
    private final Scheduler subscribeOnScheduler;
    private final LoggingInfo loggingInfo;

    public SequentialFluxSubscriber(I input,
                                    Function<I, Flux<R>> processingFluxCreator,
                                    @Nullable Consumer<? super R> subscribeConsumer,
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
    public void subscribe(Runnable onTerminateHook, Runnable onCancelHook, long itemEnqueuedTs) {
        Consumer<? super R> subscribeConsumerWrapped = getSubscribeConsumerWrapped(itemEnqueuedTs);
        processingFluxCreator.apply(input)
                .doOnTerminate(onTerminateHook)
                .doOnCancel(onCancelHook)
                .subscribeOn(subscribeOnScheduler)
                .subscribe(subscribeConsumerWrapped, subscribeErrorConsumer);
    }

    private Consumer<? super R> getSubscribeConsumerWrapped(long itemEnqueuedTs) {
        Consumer<? super R> subscribeConsumerWrapper = result -> {
            subscribeConsumer.accept(result);
            final long end = System.currentTimeMillis();
            final long processingTimeMillis = end - itemEnqueuedTs;
            if (loggingInfo.logActivity) {
                loggingInfo.decorate(logger).decorateSetup(props -> props.analyticsId("input_processing_time").exec(Long.toString(processingTimeMillis))).info("Input took {} ms to process: {}", processingTimeMillis, loggingInfo.inputDescription);
            }
        };
        return subscribeConsumerWrapper;
    }


}
