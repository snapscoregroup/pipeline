package com.snapscore.pipeline.pulling;

import java.time.Duration;

class TrackedRequest {

    private final long trackedSince; // UTC timestamp
    private final FeedRequest feedRequest;

    TrackedRequest(long trackedSince, FeedRequest feedRequest) {
        this.trackedSince = trackedSince;
        this.feedRequest = feedRequest;
    }

    TrackedRequest(FeedRequest feedRequest) {
        this.trackedSince = System.currentTimeMillis();
        this.feedRequest = feedRequest;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

    public Duration trackedDuration() {
        long trackedMillis = System.currentTimeMillis() - trackedSince;
        return Duration.ofMillis(trackedMillis);
    }

    @Override
    public String toString() {
        return "TrackedRequest{" +
                "trackedSince=" + trackedSince +
                ", feedRequest=" + feedRequest +
                '}';
    }
}
