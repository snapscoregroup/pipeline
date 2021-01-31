package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.logging.Logger;
import com.snapscore.pipeline.pulling.FeedRequest;
import okhttp3.Call;
import okhttp3.Response;
import okhttp3.ResponseBody;
import reactor.core.publisher.MonoSink;

import java.io.IOException;

public class OkHttpClientCallbackImpl extends AbstractClientCallback implements OkHttpClientCallback {

    private static final Logger logger = Logger.setup(OkHttpClientCallbackImpl.class);

    OkHttpClientCallbackImpl(PullingStatisticsService pullingStatisticsService,
                             FeedRequest feedRequest,
                             MonoSink<byte[]> emitter) {
        super(feedRequest, emitter, pullingStatisticsService);
    }

    @Override
    public void onResponse(Call call, Response response) {
        try {
            if (response.isSuccessful()) {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("request_success")).info("Successful response for request: {}", feedRequest.toStringBasicInfo());
                ResponseBody responseBody = response.body();
                if (responseBody != null) {
                    handleSuccessfulResponse(responseBody.bytes());
                } else {
                    logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("Received responseBody is null! {}", feedRequest.toStringBasicInfo());
                    emitFailedRequestException();
                }
                pullingStatisticsService.ifPresent(service -> service.recordSuccessfulPullFor(feedRequest.getFeedName()));
            } else {
                handleUnsuccessfulResponse(response.code());
            }
        } catch (java.net.SocketTimeoutException e) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("Request timeout for: {}", feedRequest.toStringBasicInfo(), e);
            pullingStatisticsService.ifPresent(service -> service.recordFailedPullFor(feedRequest.getFeedName()));
            emitFailedRequestException();
        } catch (Exception e) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while processing response for: {}", feedRequest.toStringBasicInfo(), e);
            emitFailedRequestException();
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (Exception e) {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error closing the response! ", e);
            }
        }
    }

    @Override
    public void onFailure(Call call, IOException e) {
        super.handleException(e);
    }

}
