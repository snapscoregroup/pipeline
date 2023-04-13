package com.snapscore.pipeline.pulling;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Schedules dynamic and fixed feedRequests pulling.
 * 'Dynamic' means that each time requests are created from scratch and then pulled each time (used for urls with changing parameters like dates & timestamps)
 * 'Fixed' Means that the request is always pulled as it was first defined
 */
public interface PullingScheduler {

    /**
     * Schedules pulling at given interval. FeedRequest instances are created dynamically every time -> if the feed url is changing (e.g. due to a timestamp or date),
     * the specified feedRequestSupplier is responsible for returning the relevant version of url within a FeedRequest instance.
     *
     * @param scheduledPullingKey a unique key chosen to identify all the created scheduledPullings as a group
     * @param feedRequestSupplier supplies the requests to be pulled at the specified interval.
     * @param pullResultConsumer  consumes and processes the pulled data - if there are any
     * @param pullErrorConsumer
     * @param pullInterval        interval at which all supplied requests are pulled.
     * @param initialDelay        initial delay before pulling can start
     * @return DynamicScheduledRequests which can be used to cancel the scheduled pulling
     */
    <K> ScheduledDynamicRequests<K> schedulePullingDynamicRequests(K scheduledPullingKey,
                                                                   Supplier<List<FeedRequest>> feedRequestSupplier,
                                                                   Consumer<PullResult> pullResultConsumer,
                                                                   Consumer<PullError> pullErrorConsumer,
                                                                   Duration pullInterval,
                                                                   Duration initialDelay);

    /**
     * Schedules pulling at the interval provided by the specified feedRequest.
     *
     * @param scheduledPullingKey a unique key chosen to identify the created one scheduledPulling
     * @param pullResultConsumer  consumes and processes the pulled data - if there are any
     * @param pullErrorConsumer
     * @param initialDelay        initial delay before pulling can start
     * @return DynamicScheduledRequests which can be used to cancel the scheduled pulling
     */
    <K> ScheduledFixedRequest<K> schedulePullingFixedRequest(K scheduledPullingKey,
                                                             FeedRequestWithInterval feedRequest,
                                                             Consumer<PullResult> pullResultConsumer,
                                                             Consumer<PullError> pullErrorConsumer,
                                                             Duration initialDelay);


    /**
     * Schedules pulling at given interval all the specified FeedRequests.
     *
     * @param scheduledPullingKey a unique key chosen to identify all the created the scheduledPulling of the group of specified feedRequests
     * @param feedRequests        the requests to be pulled at the specified interval.
     * @param pullResultConsumer  consumes and processes the pulled data - if there are any
     * @param pullErrorConsumer
     * @param pullInterval        interval at which all supplied requests are pulled.
     * @param initialDelay        initial delay before pulling can start
     * @return DynamicScheduledRequests which can be used to cancel the scheduled pulling
     */
    <K> ScheduledFixedRequests<K> schedulePullingFixedRequests(K scheduledPullingKey,
                                                               List<FeedRequest> feedRequests,
                                                               Consumer<PullResult> pullResultConsumer,
                                                               Consumer<PullError> pullErrorConsumer,
                                                               Duration pullInterval,
                                                               Duration initialDelay);

    /**
     * Asynchronously pulls data for the specified feedRequest as soon as possible
     *  @param feedRequest
     * @param pullResultConsumer
     * @param pullErrorConsumer
     */
    void pullOnce(FeedRequest feedRequest,
                  Consumer<PullResult> pullResultConsumer,
                  Consumer<PullError> pullErrorConsumer);


    /**
     * Can be used for pulling some data for a predefined number of times after which pulling will stop
     * @return
     */
    <K> ScheduledFixedBoundedRequest<K> pullNumberOfTimes(K scheduledPullingKey,
                                                          FeedRequestWithInterval feedRequest,
                                                          Consumer<PullResult> pullResultConsumer,
                                                          Consumer<PullError> pullErrorConsumer,
                                                          int repeatTimes,
                                                          Duration initialDelay);


}
