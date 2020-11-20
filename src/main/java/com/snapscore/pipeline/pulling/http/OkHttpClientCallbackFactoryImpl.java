package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;
import reactor.core.publisher.MonoSink;

public class OkHttpClientCallbackFactoryImpl implements ClientCallbackFactory<OkHttpClientCallback> {

    private final PullingStatisticsService pullingStatisticsService;

    public OkHttpClientCallbackFactoryImpl(PullingStatisticsService pullingStatisticsService) {
        this.pullingStatisticsService = pullingStatisticsService;
    }

    @Override
    public OkHttpClientCallback createCallback(FeedRequest feedRequest, MonoSink<byte[]> emitter) {
        return new OkHttpClientCallbackImpl(pullingStatisticsService, feedRequest, emitter);
    }
}
