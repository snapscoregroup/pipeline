package com.snapscore.pipeline.pulling;

import org.junit.Test;

public class RetryDelaySupplierRandomisedTest {

    @Test
    public void calcBackoff() {

        RetryDelaySupplier retryDelaySupplier = new RetryDelaySupplierRandomised(1000);
        FeedRequest feedRequest = new FeedRequest(null, null, null, 5, null, null, null);
        System.out.println(retryDelaySupplier.calcBackoff(feedRequest));

    }


}