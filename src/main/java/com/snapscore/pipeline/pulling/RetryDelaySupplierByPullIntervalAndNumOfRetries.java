package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;

import java.time.Duration;

public class RetryDelaySupplierByPullIntervalAndNumOfRetries implements RetryDelaySupplier {

    private static final Logger logger = Logger.setup(RetryDelaySupplierByPullIntervalAndNumOfRetries.class);

    public static final RetryDelaySupplierByPullIntervalAndNumOfRetries INSTANCE = new RetryDelaySupplierByPullIntervalAndNumOfRetries();

    /**
     * @param feedRequest instance of FeedRequestWithInterval
     */
    @Override
    public Duration calcBackoff(FeedRequest feedRequest) {
        if (feedRequest instanceof FeedRequestWithInterval) {
            Duration pullInterval = ((FeedRequestWithInterval) feedRequest).getPullInterval();
            long pow = (long) Math.pow(feedRequest.getNumOfRetries(), 2.0);
            long divisor = pow * 2L;
            if (divisor == 0L) {
                divisor = 4L; // some number
            }
            Duration backoff = pullInterval.dividedBy(divisor);
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).debug("Retry backoff for request {}; request: {}", backoff, feedRequest.toStringBasicInfo()); // this is logged upon flux construction (not while it is pullig data)
            return backoff;
        } else {
            return DEFAULT_FIRST_RETRY_BACKOFF;
        }
    }

}
