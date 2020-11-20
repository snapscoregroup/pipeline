package com.snapscore.pipeline.pulling;

import reactor.core.Disposable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ScheduledFixedRequests<K> extends ScheduledPulling<K> {

    private final List<FeedRequest> feedRequests;

    public ScheduledFixedRequests(K scheduledPullingKey,
                                  Disposable disposable,
                                  Duration pullInterval,
                                  List<FeedRequest> feedRequests) {
        super(scheduledPullingKey, disposable, pullInterval);
        this.feedRequests = new ArrayList<>(feedRequests);
    }

    public List<FeedRequest> getFeedRequests() {
        return feedRequests;
    }

}
