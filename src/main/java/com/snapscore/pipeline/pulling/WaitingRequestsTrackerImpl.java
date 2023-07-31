package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WaitingRequestsTrackerImpl implements WaitingRequestsTracker {

    private static final Logger logger = Logger.setup(ScheduledPullingCacheImpl.class);

    private final Function<FeedRequest, String> deduplicatingFeedRequestKeyMaker;
    private final ConcurrentMap<String, TrackedRequest> requestsAwaitingToBePulledByUrlMap = new ConcurrentHashMap<>();

    /**
     * @param deduplicatingFeedRequestKeyMaker must return a string that will be used as an identifier
     *                                         for the tracked request and to filter out duplicate requests
     */
    public WaitingRequestsTrackerImpl(Function<FeedRequest, String> deduplicatingFeedRequestKeyMaker) {
        this.deduplicatingFeedRequestKeyMaker = deduplicatingFeedRequestKeyMaker;
    }

    @Override
    public boolean isAwaitingResponse(FeedRequest feedRequest) {
        try {
            return requestsAwaitingToBePulledByUrlMap.containsKey(makeKey(feedRequest));
        } catch (Exception e) {
            logger.error("Error! {}", feedRequest, e);
            return false;
        }
    }

    @Override
    public void trackAwaitingResponse(FeedRequest feedRequest) {
        try {
            requestsAwaitingToBePulledByUrlMap.put(makeKey(feedRequest), new TrackedRequest(feedRequest));
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("tracking_request")).info("Tracking feedRequest: {}", feedRequest.toStringBasicInfo());
        } catch (Exception e) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while tracking request! {}", feedRequest, e);
        }
    }

    @Override
    public Optional<TrackedRequest> getTrackedRequest(FeedRequest feedRequest) {
        TrackedRequest trackedRequest = requestsAwaitingToBePulledByUrlMap.get(makeKey(feedRequest));
        return Optional.ofNullable(trackedRequest);
    }

    @Override
    public void untrackProcessed(FeedRequest feedRequest) {
        try {
            TrackedRequest removedRq = requestsAwaitingToBePulledByUrlMap.remove(makeKey(feedRequest));
            if (removedRq == null) {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("FeedRequest not present in the tracker but it should be - nothing to untrack: {}", feedRequest.toStringBasicInfo());
            } else {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).info("Untracked feedRequest: {}", feedRequest.toStringBasicInfo());
            }
        } catch (Exception e) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while untracking request! {}", feedRequest, e);
        }
    }

    @Override
    public int countOfRequestsAwaitingResponse() {
        return requestsAwaitingToBePulledByUrlMap.size();
    }

    @Override
    public SortedMap<FeedPriorityEnum, Long> countOfRequestsByPriority() {
        final Map<FeedPriorityEnum, Long> collect = requestsAwaitingToBePulledByUrlMap.values().stream()
                .map(trackedRequest -> trackedRequest.getFeedRequest().priority)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        SortedMap<FeedPriorityEnum, Long> result = new TreeMap<>();
        Arrays.stream(FeedPriorityEnum.values())
                .forEach(priority -> result.put(priority, collect.getOrDefault(priority, 0L)));
        return result;
    }

    private String makeKey(FeedRequest feedRequest) {
        try {
            return deduplicatingFeedRequestKeyMaker.apply(feedRequest);
        } catch (Exception e) {
            logger.error("Error while making feedRequest key! Will use request url! {}", feedRequest, e);
            return feedRequest.getUrl();
        }
    }

}
