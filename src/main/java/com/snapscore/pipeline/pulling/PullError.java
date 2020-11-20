package com.snapscore.pipeline.pulling;

public class PullError {

    private final FeedRequest feedRequest;
    private final Throwable error;

    public PullError(FeedRequest feedRequest, Throwable error) {
        this.feedRequest = feedRequest;
        this.error = error;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

    public Throwable getError() {
        return error;
    }

    @Override
    public String toString() {
        return "PullError{" +
                "feedRequest=" + feedRequest +
                ", error=" + error +
                '}';
    }
}
