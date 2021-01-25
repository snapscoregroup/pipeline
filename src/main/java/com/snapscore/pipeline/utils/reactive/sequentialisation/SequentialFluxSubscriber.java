package com.snapscore.pipeline.utils.reactive.sequentialisation;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

public class SequentialFluxSubscriber<M, T> {

    private final static Logger logger = Logger.setup(SequentialFluxSubscriber.class);

    private final M message;
    private final Function<M, Flux<T>> processingFluxCreator;
    private final Consumer<? super T> subscribeConsumer;
    private final Consumer<? super Throwable> subscribeErrorConsumer;
    private final LoggingInfo loggingInfo;

    public SequentialFluxSubscriber(M message,
                                    Function<M, Flux<T>> processingFluxCreator,
                                    @Nullable Consumer<? super T> subscribeConsumer,
                                    Consumer<? super Throwable> subscribeErrorConsumer,
                                    LoggingInfo loggingInfo) {
        this.message = message;
        this.processingFluxCreator = processingFluxCreator;
        this.subscribeConsumer = subscribeConsumer;
        this.subscribeErrorConsumer = subscribeErrorConsumer;
        this.loggingInfo = loggingInfo;
    }

    void subscribe(Runnable onTerminateHook, Runnable onCancelHook) {
        Consumer<? super T> subscribeConsumerWrapped = getSubscribeConsumerWrapped();
        processingFluxCreator.apply(message)
                .doOnTerminate(onTerminateHook)
                .doOnCancel(onCancelHook)
                .subscribe(subscribeConsumerWrapped, subscribeErrorConsumer);
    }

    private Consumer<? super T> getSubscribeConsumerWrapped() {
        final long start = System.currentTimeMillis();
        Consumer<? super T> subscribeConsumerWrapper = result -> {
            subscribeConsumer.accept(result);
            final long end = System.currentTimeMillis();
            final long processingTimeMillis = end - start;
            loggingInfo.decorate(logger).info("Message took {}ms to process: {}", processingTimeMillis, loggingInfo.getMessage());
        };
        return subscribeConsumerWrapper;
    }


}
