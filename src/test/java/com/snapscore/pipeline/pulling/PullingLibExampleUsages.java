package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.pulling.http.HttpClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.snapscore.pipeline.pulling.TestData.MATCH_DETAIL_FEED_NAME;
import static org.junit.Assert.assertEquals;

public class PullingLibExampleUsages {

    private static final Logger log = LoggerFactory.getLogger(PullingLibExampleUsages.class);

    private static final String MATCH_ID_1 = "match id 1";
    private static final String MATCH_ID_1_DATA = "match id 1 data";

    private HttpClient httpClientMock;
    private PullingScheduler pullingScheduler;
    private WaitingRequestsTracker waitingRequestsTracker;


    @Before
    public void setUp() throws Exception {
        httpClientMock = new HttpClientMock(MATCH_ID_1_DATA);
        this.waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        PullingSchedulerQueue pullingSchedulerQueue = new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);
    }

    // a mocked cliend that will always return the data we instantiate it with
    private static class HttpClientMock implements HttpClient {

        private String dataToReturn;

        public HttpClientMock(String dataToReturn) {
            this.dataToReturn = dataToReturn;
        }

        @Override
        public CompletableFuture<byte[]> getAsync(FeedRequest feedRequest) {
            CompletableFuture<byte[]> result = new CompletableFuture<>();
            result.complete(dataToReturn.getBytes());
            return result;
        }

        @Override
        public void shutdown() {

        }
    }

    // just an a very simple example data processing class
    private static class MatchHandler {

        public void processMatchDetail(PullResult pullResult) {
            log.info("Processing pulled match detail data {} from feedRequest {}", new String(pullResult.getData()), pullResult.getFeedRequest());
            // process the data ...
        }

    }

    // example definition of a ScheduledPullingKey identifying the scheduled pulling of a feed with match data
    private static class MatchDataScheduledPullingKey {
        private FeedName feedName;
        private String matchId;

        public MatchDataScheduledPullingKey(FeedName feedName, String matchId) {
            this.feedName = feedName;
            this.matchId = matchId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MatchDataScheduledPullingKey)) return false;
            MatchDataScheduledPullingKey that = (MatchDataScheduledPullingKey) o;
            return Objects.equals(feedName, that.feedName) &&
                    Objects.equals(matchId, that.matchId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(feedName, matchId);
        }
    }



    @Test
    public void pullingMatchDetailsOnceAndPassingThemToBeProcessed() throws InterruptedException {

        // pulled data consumer / processor
        final MatchHandler matchHandlerMock = Mockito.mock(MatchHandler.class);
        final Consumer<PullResult> pullResultConsumer = pullResult -> {
            matchHandlerMock.processMatchDetail(pullResult);
        };

        // prepare feedRequest and schedule its pulling
        final FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.LOW,5, "url_for_match_detail").build();
        final HttpClient httpClientMock = new HttpClientMock(MATCH_ID_1_DATA);
        final RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR);
        final PullingScheduler pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);
        pullingScheduler.pullOnce(feedRequest, pullResultConsumer, pullError -> {} );

        Thread.sleep(100);

        // verify that data was pulled and passed to specialised handler for processing
        final ArgumentCaptor<PullResult> argumentCaptor = ArgumentCaptor.forClass(PullResult.class);
        Mockito.verify(matchHandlerMock, Mockito.times(1)).processMatchDetail(argumentCaptor.capture());
        final String processedData = new String((byte[]) argumentCaptor.getValue().getData());

        assertEquals(MATCH_ID_1_DATA, processedData);
    }


    @Test
    public void schedulePullingOneFeedUrlForSomeTimeAndThenCancelThePulling() throws InterruptedException {

        // pulled data consumer / processor
        final MatchHandler matchHandlerMock = Mockito.mock(MatchHandler.class);
        final Consumer<PullResult> pullResultConsumer = pullResult -> {
            matchHandlerMock.processMatchDetail(pullResult);
        };

        // prepare feedRequest and schedule its pulling
        final Duration pullInterval = Duration.ofMillis(1);
        final FeedRequestWithInterval feedRequest = FeedRequestWithInterval.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.LOW,5, "url_for_match_detail", pullInterval).build();
        final MatchDataScheduledPullingKey scheduledPullingKey = new MatchDataScheduledPullingKey(feedRequest.getFeedName(), MATCH_ID_1);
        final ScheduledFixedRequest<MatchDataScheduledPullingKey> scheduledFixedRequest = pullingScheduler.schedulePullingFixedRequest(scheduledPullingKey, feedRequest, pullResultConsumer, pullError -> {}, Duration.ZERO);

        Thread.sleep(100); // let pulling run for a while

        // cancelling the scheduled request
        scheduledFixedRequest.cancel();

        // verify that data was pulled multiple times and passed to specialised handler for processing
        final ArgumentCaptor<PullResult> argumentCaptor = ArgumentCaptor.forClass(PullResult.class);
        Mockito.verify(matchHandlerMock, Mockito.atLeast(20)).processMatchDetail(argumentCaptor.capture());
        for (PullResult pullResult : argumentCaptor.getAllValues()) {
            final String processedData = new String((byte[]) pullResult.getData());
            assertEquals(MATCH_ID_1_DATA, processedData);
        }
    }

    @Ignore
    @Test
    public void cachingScheduledPullingSoWeCanChangeThePullingFrequencyLater() throws InterruptedException {

        // cache to hold all scheduled pullings with the given key
        ScheduledPullingCache<MatchDataScheduledPullingKey, ScheduledFixedRequest<MatchDataScheduledPullingKey>> scheduledPullingCache = new ScheduledPullingCacheImpl<>();

        // pulled data consumer / processor
        final MatchHandler matchHandlerMock = Mockito.mock(MatchHandler.class);
        final Consumer<PullResult> pullResultConsumer = pullResult -> {
            matchHandlerMock.processMatchDetail(pullResult);
        };

        // prepare feedRequest and schedule its pulling
        // this will be pulled at high frequency
        final Duration pullInterval = Duration.ofMillis(1);
        final FeedRequestWithInterval feedRequest = FeedRequestWithInterval.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.LOW,5, "url_for_match_detail", pullInterval).build();
        MatchDataScheduledPullingKey scheduledPullingKey = new MatchDataScheduledPullingKey(feedRequest.getFeedName(), MATCH_ID_1);
        ScheduledFixedRequest<MatchDataScheduledPullingKey> scheduledFixedRequest = pullingScheduler.schedulePullingFixedRequest(scheduledPullingKey, feedRequest, pullResultConsumer, pullError -> {}, Duration.ZERO);

        // cache the scheduled pulling for later access / manipulation
        scheduledPullingCache.setScheduledPulling(scheduledFixedRequest);

        Thread.sleep(100); // let pulling run for a while

        // check that it was called at least 50 times when pulled with high frequency
        Mockito.verify(matchHandlerMock, Mockito.atLeast(20)).processMatchDetail(Mockito.any());

        // CHANGING PULLING FREQUENCY ...

        // prepare new feedRequest for the same feed and match - this time with lower pulling frequency
        final Duration pullIntervalNew = Duration.ofMillis(10);
        final FeedRequestWithInterval feedRequestNew = FeedRequestWithInterval.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.LOW,5, "url_for_match_detail", pullIntervalNew).build();
        ScheduledFixedRequest<MatchDataScheduledPullingKey> scheduledFixedRequestNew = pullingScheduler.schedulePullingFixedRequest(scheduledPullingKey, feedRequestNew, pullResultConsumer, pullError -> {}, Duration.ZERO);

//        scheduledPullingCache.cancelScheduledPulling(scheduledPullingKey); // this step is optional - setting new ScheduledPulling with this key will cancel the currently cached one
        // this replaces the previously scheduled
        scheduledPullingCache.setScheduledPulling(scheduledFixedRequestNew);
        Mockito.reset(matchHandlerMock); // reset so we see only interactions after the pulling was cancelled

        Thread.sleep(100); // let pulling run for a while

        // verify that after the
        Mockito.verify(matchHandlerMock, Mockito.atLeast(5)).processMatchDetail(Mockito.any());
        Mockito.verify(matchHandlerMock, Mockito.atMost(15)).processMatchDetail(Mockito.any());
    }

}
