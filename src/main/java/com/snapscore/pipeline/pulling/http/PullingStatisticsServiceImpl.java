package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedName;
import com.snapscore.pipeline.utils.DateUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PullingStatisticsServiceImpl implements PullingStatisticsService {

    private final ConcurrentMap<FeedName, LocalDateTime> lastSuccesfulPullByFeedName = new ConcurrentHashMap<>();
    private final ConcurrentMap<FeedName, LocalDateTime> lastFailedPullByFeedName = new ConcurrentHashMap<>();

    public PullingStatisticsServiceImpl() {
    }

    @Override
    public void recordSuccessfulPullFor(FeedName feedName) {
        lastSuccesfulPullByFeedName.put(feedName, DateUtils.nowUTC());
    }

    @Override
    public void recordFailedPullFor(FeedName feedName) {
        lastFailedPullByFeedName.put(feedName, DateUtils.nowUTC());
    }

    @Override
    public LocalDateTime getLastSuccessfulPullDt(FeedName feedName) {
        return lastSuccesfulPullByFeedName.get(feedName);
    }

    @Override
    public LocalDateTime getLastFailedPullDt(FeedName feedName) {
        return lastFailedPullByFeedName.get(feedName);
    }

}
