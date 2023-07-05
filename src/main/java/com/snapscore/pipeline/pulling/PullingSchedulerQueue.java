package com.snapscore.pipeline.pulling;

import java.util.List;
import java.util.function.Consumer;

public interface PullingSchedulerQueue {

    /**
     * Enqueues the request to be dequeued at some later point at regular frequencies defined by the implementation
     * in the order of the request priorities
     */
    void enqueueForPulling(FeedRequest feedRequest,
                           Consumer<PullResult> pullResultConsumer,
                           Consumer<PullError> pullErrorConsumer);

    List<FeedRequest> getCurrentQueue();
    

}
