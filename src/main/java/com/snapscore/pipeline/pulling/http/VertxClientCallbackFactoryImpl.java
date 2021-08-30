package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;
import reactor.core.publisher.MonoSink;

import java.util.List;

public class VertxClientCallbackFactoryImpl implements ClientCallbackFactory<VertxClientCallback> {

    private final PullingStatisticsService pullingStatisticsService;
    private final int httpResponseBufferSize;
    private final List<HeadersObserver> headersObservers;

    public VertxClientCallbackFactoryImpl(PullingStatisticsService pullingStatisticsService,
                                          int httpResponseBufferSize,
                                          List<HeadersObserver> headersObservers) {
        this.pullingStatisticsService = pullingStatisticsService;
        this.httpResponseBufferSize = httpResponseBufferSize;
        this.headersObservers = headersObservers;
    }

    public VertxClientCallbackFactoryImpl(int httpResponseBufferSize) {
        this(null, httpResponseBufferSize, List.of());
    }

    @Override
    public VertxClientCallback createCallback(FeedRequest feedRequest, MonoSink<byte[]> emitter) {
        return new VertxClientCallbackImpl(pullingStatisticsService, feedRequest, emitter, httpResponseBufferSize, headersObservers);
    }
}
