package com.snapscore.pipeline.pulling;

import java.time.Duration;

public interface RetryDelaySupplier {

    final Duration DEFAULT_FIRST_RETRY_BACKOFF = Duration.ofSeconds(20);

    Duration calcBackoff(FeedRequest feedRequest);

}
