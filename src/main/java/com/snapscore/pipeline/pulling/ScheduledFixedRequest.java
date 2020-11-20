package com.snapscore.pipeline.pulling;

import reactor.core.Disposable;

public class ScheduledFixedRequest<K> extends ScheduledPulling<K> {

    private final FeedRequestWithInterval feedRequest;

    public ScheduledFixedRequest(K scheduledPullingKey,
                                 FeedRequestWithInterval feedRequest,
                                 Disposable disposable) {
        super(scheduledPullingKey, disposable, feedRequest.getPullInterval());
        this.feedRequest = feedRequest;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

}
