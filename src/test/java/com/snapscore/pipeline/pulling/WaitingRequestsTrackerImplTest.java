package com.snapscore.pipeline.pulling;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class WaitingRequestsTrackerImplTest {

    @Test
    public void countOfRequestsByPriority() {
        // given
        WaitingRequestsTrackerImpl tracker = new WaitingRequestsTrackerImpl(FeedRequest::getUrl);

        // when
        tracker.trackAwaitingResponse(createRequest(FeedPriorityEnum.LOW, "1"));
        tracker.trackAwaitingResponse(createRequest(FeedPriorityEnum.LOW, "2"));
        tracker.trackAwaitingResponse(createRequest(FeedPriorityEnum.MEDIUM, "3"));
        final Map<FeedPriorityEnum, Long> count = tracker.countOfRequestsByPriority();

        // then
        assertEquals(2L, (long) count.get(FeedPriorityEnum.LOW));
        assertEquals(1L, (long) count.get(FeedPriorityEnum.MEDIUM));
        assertEquals(0L, (long) count.get(FeedPriorityEnum.HIGH));
    }

    private FeedRequest createRequest(FeedPriorityEnum priority, String index) {
        return new FeedRequest(null, "some_url" + index, priority, 1, null, null, Collections.emptyList());
    }

}