package com.snapscore.pipeline.pulling;

import org.junit.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static org.junit.Assert.*;

public class RetryDelaySupplierByPullIntervalAndNumOfRetriesTest {

    @Test
    public void calcBackoff() {

        RetryDelaySupplier retryDelaySupplier = new RetryDelaySupplierByPullIntervalAndNumOfRetries();
        FeedRequestWithInterval feedRequest = new FeedRequestWithInterval(null, null, null, Duration.ofSeconds(60), 5, null, null, null);
        System.out.println(retryDelaySupplier.calcBackoff(feedRequest));


    }
}