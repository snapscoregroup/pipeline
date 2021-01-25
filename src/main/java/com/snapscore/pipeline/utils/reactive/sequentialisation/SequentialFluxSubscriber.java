package com.snapscore.pipeline.utils.reactive.sequentialisation;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

public class SequentialFluxSubscriber<I, R> {

    private final static Logger logger = Logger.setup(SequentialFluxSubscriber.class);

    private final I input;
    private final Function<I, Flux<R>> processingFluxCreator;
    private final Consumer<? super R> subscribeConsumer;
    private final Consumer<? super Throwable> subscribeErrorConsumer;
    private final LoggingInfo loggingInfo;

    public SequentialFluxSubscriber(I input,
                                    Function<I, Flux<R>> processingFluxCreator,
                                    @Nullable Consumer<? super R> subscribeConsumer,
                                    Consumer<? super Throwable> subscribeErrorConsumer,
                                    LoggingInfo loggingInfo) {
        this.input = input;
        this.processingFluxCreator = processingFluxCreator;
        this.subscribeConsumer = subscribeConsumer;
        this.subscribeErrorConsumer = subscribeErrorConsumer;
        this.loggingInfo = loggingInfo;
    }

    void subscribe(Runnable onTerminateHook, Runnable onCancelHook) {
        Consumer<? super R> subscribeConsumerWrapped = getSubscribeConsumerWrapped();
        processingFluxCreator.apply(input)
                .doOnTerminate(onTerminateHook)
                .doOnCancel(onCancelHook)
                .subscribe(subscribeConsumerWrapped, subscribeErrorConsumer);
    }

    private Consumer<? super R> getSubscribeConsumerWrapped() {
        final long start = System.currentTimeMillis();
        Consumer<? super R> subscribeConsumerWrapper = result -> {
            subscribeConsumer.accept(result);
            final long end = System.currentTimeMillis();
            final long processingTimeMillis = end - start;
            loggingInfo.decorate(logger).info("Input took {} ms to process: {}", processingTimeMillis, loggingInfo.getMessage());
        };
        return subscribeConsumerWrapper;
    }


}
