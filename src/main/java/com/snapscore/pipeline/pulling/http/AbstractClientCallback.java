package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.logging.Logger;
import com.snapscore.pipeline.pulling.FeedRequest;
import reactor.core.publisher.MonoSink;

import java.net.SocketTimeoutException;
import java.util.Optional;

public abstract class AbstractClientCallback implements ClientCallback {

    private static final Logger logger = Logger.setup(AbstractClientCallback.class);

    protected final FeedRequest feedRequest;
    protected final MonoSink<byte[]> emitter;
    protected final Optional<PullingStatisticsService> pullingStatisticsService;

    public AbstractClientCallback(FeedRequest feedRequest,
                                  MonoSink<byte[]> emitter,
                                  PullingStatisticsService pullingStatisticsService) {
        this.feedRequest = feedRequest;
        this.emitter = emitter;
        this.pullingStatisticsService = Optional.ofNullable(pullingStatisticsService);
    }

    public AbstractClientCallback(FeedRequest feedRequest,
                                  MonoSink<byte[]> emitter) {
        this(feedRequest, emitter, null);
    }

    @Override
    public void handleSuccessfulResponse(byte[] responseData) {
        if (responseData != null) {
            emitResponseData(responseData);
        } else {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("Received data from responseData is null! {}", feedRequest.toStringBasicInfo());
            emitFailedRequestException();
        }
    }

    @Override
    public void handleUnsuccessfulResponse(int statusCode) {
        logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("resp_status_" + statusCode)).warn("Request not successful, response code = {}; request: {}", statusCode, feedRequest.toStringBasicInfo());
        pullingStatisticsService.ifPresent(service -> service.recordFailedPullFor(feedRequest.getFeedName()));
        emitFailedRequestException();
    }

    @Override
    public void handleException(Throwable e) {
        if (e instanceof SocketTimeoutException) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).info("Request timeout for: {}", feedRequest);
        } else {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("Request {} failed with error; Error:", feedRequest.getUuid(), e);
        }
        emitFailedRequestException();
    }

    @Override
    public void emitResponseData(byte[] responseData) {
        emitter.success(responseData);
    }

    @Override
    public void emitFailedRequestException() {
        emitter.error(new FailedRequestException(feedRequest));
    }

}
