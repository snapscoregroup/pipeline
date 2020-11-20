package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedName;

import java.time.LocalDateTime;

public interface PullingStatisticsService {

    void recordSuccessfulPullFor(FeedName feedName);

    void recordFailedPullFor(FeedName feedName);

    LocalDateTime getLastSuccessfulPullDt(FeedName feedName);

    LocalDateTime getLastFailedPullDt(FeedName feedName);

}
