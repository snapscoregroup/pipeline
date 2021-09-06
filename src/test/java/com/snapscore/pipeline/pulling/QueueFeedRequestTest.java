package com.snapscore.pipeline.pulling;

import org.junit.Test;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.junit.Assert.assertEquals;

public class QueueFeedRequestTest {

    @Test
    public void testQueuingFeedRequests() throws InterruptedException {

        QueueFeedRequest rq1 = createRequest(FeedPriorityEnum.MEDIUM);
        QueueFeedRequest rq2 = createRequest(FeedPriorityEnum.HIGH);
        Thread.sleep(1); // Make sure there is a slight delay between the requests so their createdDt is different
        QueueFeedRequest rq3 = createRequest(FeedPriorityEnum.MEDIUM);
        QueueFeedRequest rq4 = createRequest(FeedPriorityEnum.HIGH);

        Queue<QueueFeedRequest> requestsQueue = new PriorityBlockingQueue<>(100, QueueFeedRequest.makeComparatorFrom(FeedRequest.DEFAULT_PRIORITY_COMPARATOR));

        requestsQueue.add(rq1);
        requestsQueue.add(rq2);
        requestsQueue.add(rq3);
        requestsQueue.add(rq4);

        assertEquals(rq2, requestsQueue.poll());
        assertEquals(rq4, requestsQueue.poll());
        assertEquals(rq1, requestsQueue.poll());
        assertEquals(rq3, requestsQueue.poll());
    }

    private QueueFeedRequest createRequest(FeedPriorityEnum priority) {
        FeedRequest feedRequest = new FeedRequest(null, "some_url", priority, 1, null, null, Collections.emptyList());
        return new QueueFeedRequest(feedRequest, null, null, System.currentTimeMillis());
    }


}
