package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;
import reactor.core.publisher.MonoSink;

public class VertxClientCallbackFactoryImpl implements ClientCallbackFactory<VertxClientCallback> {

    private final PullingStatisticsService pullingStatisticsService;
    private final int httpResponseBufferSize;

    public VertxClientCallbackFactoryImpl(PullingStatisticsService pullingStatisticsService,
                                          int httpResponseBufferSize) {
        this.pullingStatisticsService = pullingStatisticsService;
        this.httpResponseBufferSize = httpResponseBufferSize;
    }

    public VertxClientCallbackFactoryImpl(int httpResponseBufferSize) {
        this(null, httpResponseBufferSize);
    }

    @Override
    public VertxClientCallback createCallback(FeedRequest feedRequest, MonoSink<byte[]> emitter) {
        return new VertxClientCallbackImpl(pullingStatisticsService, feedRequest, emitter, httpResponseBufferSize);
    }
}
