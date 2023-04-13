package com.snapscore.pipeline.pulling;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.function.Consumer;

public class QueueFeedRequest {

    private final FeedRequest feedRequest;
    private final Consumer<PullResult> pullResultConsumer;
    private final Consumer<PullError> pullErrorConsumer;
    private final long enqueuedTimestamp;

    public QueueFeedRequest(FeedRequest feedRequest,
                            Consumer<PullResult> pullResultConsumer,
                            Consumer<PullError> pullErrorConsumer,
                            long enqueuedTimestamp) {
        this.feedRequest = feedRequest;
        this.pullResultConsumer = pullResultConsumer;
        this.pullErrorConsumer = pullErrorConsumer;
        this.enqueuedTimestamp = enqueuedTimestamp;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

    public Consumer<PullResult> getPullResultConsumer() {
        return pullResultConsumer;
    }

    public Consumer<PullError> getPullErrorConsumer() {
        return pullErrorConsumer;
    }

    public long getEnqueuedTimestamp() {
        return enqueuedTimestamp;
    }

    public int getSchedulingOrder() {
        return feedRequest.getPriority().getSchedulingOrder();
    }

    public LocalDateTime getCreatedDt() {
        return feedRequest.createdDt;
    }

    public static Comparator<QueueFeedRequest> makeComparatorFrom(Comparator<FeedRequest> feedRequestComparator) {
        return (qr1, qr2) -> feedRequestComparator.compare(qr1.getFeedRequest(), qr2.getFeedRequest());
    }

}
