package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;

import java.util.Comparator;

/**
 * Default implementation. Client app can implement its own logic of e.g. based on custom feed request property values
 * allowing fine-grained control over which request are more important.
 */
public class FeedRequestDefaultPriorityComparator implements Comparator<FeedRequest> {

    private static final Logger logger = Logger.setup(FeedRequestDefaultPriorityComparator.class);

    public static final Comparator<FeedRequest> FEED_REQUEST_PRIORITY_COMPARATOR = Comparator
            .comparingInt((FeedRequest feedRequest) -> feedRequest.getPriority().getSchedulingOrder())
            .thenComparingInt(FeedRequest::getSchedulingOrder)
            .thenComparing(FeedRequest::getCreatedDt);


    @Override
    public int compare(FeedRequest rq1, FeedRequest rq2) {
        try {
            return FEED_REQUEST_PRIORITY_COMPARATOR.compare(rq1, rq2);
        } catch (Exception e) {
            logger.error("Error comparing pulling priority of {} and {}", rq1, rq2);
            return 0;
        }
    }

}
