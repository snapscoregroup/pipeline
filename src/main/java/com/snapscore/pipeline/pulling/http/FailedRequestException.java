package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;

// used to publish failure to the flux so it can handle the retry
public class FailedRequestException extends AbstractRequestException {

    private final FeedRequest feedRequest;

    public FailedRequestException(FeedRequest feedRequest) {
        super("FeedRequest failed: " + feedRequest.toString(), feedRequest);
        this.feedRequest = feedRequest;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

}
