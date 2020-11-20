package com.snapscore.pipeline.pulling;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static com.snapscore.pipeline.pulling.TestData.STAGE_FIXTURES_FEED_NAME;
import static org.junit.Assert.*;

public class ScheduledPullingCacheImplTest {

    private ScheduledPullingCache<StageFixturesScheduledPullingKey, ScheduledFixedRequest<StageFixturesScheduledPullingKey>> stageFixturesScheduledPullingCache;
    private final String stageId1 = "stage_id_123";
    private final String stageId2 = "stage_id_456";
    private StageFixturesScheduledPullingKey scheduledPullingKey;
    private FeedRequestWithInterval feedRequest;
    private ScheduledFixedRequest<StageFixturesScheduledPullingKey> scheduledFixedRequest;

    @Before
    public void setUp() throws Exception {
        stageFixturesScheduledPullingCache = new ScheduledPullingCacheImpl<>();

        scheduledPullingKey = new StageFixturesScheduledPullingKey(stageId1);
        feedRequest = makeFeedRequest("url_of_" + stageId1);
        scheduledFixedRequest = new ScheduledFixedRequest<>(scheduledPullingKey, feedRequest, Mockito.mock(Disposable.class));
    }

    @Test
    public void testSetScheduledPulling() {
        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequest);

        final Optional<ScheduledFixedRequest<StageFixturesScheduledPullingKey>> scheduledPullingOpt = stageFixturesScheduledPullingCache.getScheduledPulling(scheduledPullingKey);

        assertTrue(scheduledPullingOpt.isPresent());
    }

    @Test
    public void testSetScheduledPullingCancelsPullingPreviouslyMappedToSameKey() {
        // given
        final StageFixturesScheduledPullingKey scheduledPullingKey = new StageFixturesScheduledPullingKey(stageId1);
        final ScheduledFixedRequest<StageFixturesScheduledPullingKey> scheduledFixedRequestMock1 = Mockito.mock(ScheduledFixedRequest.class);
        Mockito.when(scheduledFixedRequestMock1.getScheduledPullingKey()).thenReturn(scheduledPullingKey);
        final ScheduledFixedRequest<StageFixturesScheduledPullingKey> scheduledFixedRequestMock2 = Mockito.mock(ScheduledFixedRequest.class);
        Mockito.when(scheduledFixedRequestMock2.getScheduledPullingKey()).thenReturn(scheduledPullingKey);
        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequestMock1);

        // when
        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequestMock2);

        // then
        Mockito.verify(scheduledFixedRequestMock1, Mockito.times(1)).cancel();
        assertEquals(1, stageFixturesScheduledPullingCache.size());
    }

    @Test
    public void testGetScheduledPulling() {
        Optional<ScheduledFixedRequest<StageFixturesScheduledPullingKey>> scheduledPullingOpt = stageFixturesScheduledPullingCache.getScheduledPulling(scheduledPullingKey);
        assertFalse(scheduledPullingOpt.isPresent());

        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequest);

        scheduledPullingOpt = stageFixturesScheduledPullingCache.getScheduledPulling(scheduledPullingKey);

        assertTrue(scheduledPullingOpt.isPresent());
    }

    @Test
    public void testRemoveScheduledPulling() {
        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequest);

        Optional<ScheduledFixedRequest<StageFixturesScheduledPullingKey>> scheduledPullingOpt = stageFixturesScheduledPullingCache.getScheduledPulling(scheduledPullingKey);
        assertTrue(scheduledPullingOpt.isPresent());

        Optional<ScheduledFixedRequest<StageFixturesScheduledPullingKey>> scheduledFixedRequest = stageFixturesScheduledPullingCache.removeScheduledPulling(scheduledPullingKey);
        assertTrue(scheduledFixedRequest.isPresent());
        assertFalse(scheduledFixedRequest.get().isCancelled());

        scheduledPullingOpt = stageFixturesScheduledPullingCache.getScheduledPulling(scheduledPullingKey);
        assertFalse(scheduledPullingOpt.isPresent());
    }

    @Test
    public void testCancelScheduledPulling() {
        final StageFixturesScheduledPullingKey scheduledPullingKey = new StageFixturesScheduledPullingKey(stageId1);
        final ScheduledFixedRequest<StageFixturesScheduledPullingKey> scheduledFixedRequestMock1 = Mockito.mock(ScheduledFixedRequest.class);
        Mockito.when(scheduledFixedRequestMock1.getScheduledPullingKey()).thenReturn(scheduledPullingKey);
        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequestMock1);

        stageFixturesScheduledPullingCache.cancelScheduledPulling(scheduledPullingKey);

        Mockito.verify(scheduledFixedRequestMock1, Mockito.times(1)).cancel();
        assertEquals(0, stageFixturesScheduledPullingCache.size());
    }

    @Test
    public void testCancelAllScheduledPullings() {
        // given
        final StageFixturesScheduledPullingKey scheduledPullingKey1 = new StageFixturesScheduledPullingKey(stageId1);
        final ScheduledFixedRequest<StageFixturesScheduledPullingKey> scheduledFixedRequestMock1 = Mockito.mock(ScheduledFixedRequest.class);
        Mockito.when(scheduledFixedRequestMock1.getScheduledPullingKey()).thenReturn(scheduledPullingKey1);

        final StageFixturesScheduledPullingKey scheduledPullingKey2 = new StageFixturesScheduledPullingKey(stageId2);
        final ScheduledFixedRequest<StageFixturesScheduledPullingKey> scheduledFixedRequestMock2 = Mockito.mock(ScheduledFixedRequest.class);
        Mockito.when(scheduledFixedRequestMock2.getScheduledPullingKey()).thenReturn(scheduledPullingKey2);

        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequestMock1);
        stageFixturesScheduledPullingCache.setScheduledPulling(scheduledFixedRequestMock2);
        assertEquals(2, stageFixturesScheduledPullingCache.size());

        // when
        stageFixturesScheduledPullingCache.cancelAllScheduledPulling();

        // then
        Mockito.verify(scheduledFixedRequestMock1, Mockito.times(1)).cancel();
        Mockito.verify(scheduledFixedRequestMock2, Mockito.times(1)).cancel();
        assertEquals(0, stageFixturesScheduledPullingCache.size());
    }

    private FeedRequestWithInterval makeFeedRequest(String url) {
        return FeedRequestWithInterval.newBuilder(STAGE_FIXTURES_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, url, Duration.ofMillis(1)).build();
    }


    // example scheduled pulling key ...
    private static class StageFixturesScheduledPullingKey {
        String stageId;

        public StageFixturesScheduledPullingKey(String stageId) {
            this.stageId = stageId;
        }

        public String getStageId() {
            return stageId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StageFixturesScheduledPullingKey)) return false;
            StageFixturesScheduledPullingKey that = (StageFixturesScheduledPullingKey) o;
            return Objects.equals(stageId, that.stageId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stageId);
        }
    }
}