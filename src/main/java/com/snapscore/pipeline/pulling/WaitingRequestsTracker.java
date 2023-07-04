package com.snapscore.pipeline.pulling;

import java.util.Map;
import java.util.Optional;

/**
 * Used for tracking of requests that have been passed to the HttpClient for handling.
 * Useful for filtering out duplicate requests and tracking of current number requests waiting to be processed
 */
public interface WaitingRequestsTracker {

    boolean isAwaitingResponse(FeedRequest feedRequest);

    void trackAwaitingResponse(FeedRequest feedRequest);

    Optional<TrackedRequest> getTrackedRequest(FeedRequest feedRequest);

    void untrackProcessed(FeedRequest feedRequest);

    int countOfRequestsAwaitingResponse();

    Map<FeedPriorityEnum, Long> countOfRequestsByPriority();

}
