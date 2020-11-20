package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;

// used to publish failure to the flux so it can handle the retry
public abstract class AbstractRequestException extends Throwable {

    private final FeedRequest feedRequest;

    public AbstractRequestException(String message, FeedRequest feedRequest) {
        super(message);
        this.feedRequest = feedRequest;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }
    
}
