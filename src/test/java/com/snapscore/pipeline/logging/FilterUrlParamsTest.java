package com.snapscore.pipeline.logging;

import com.snapscore.pipeline.pulling.FeedRequest;
import com.snapscore.pipeline.pulling.FeedRequestWithInterval;
import org.junit.Test;

import java.time.Duration;

import static com.snapscore.pipeline.pulling.FeedPriorityEnum.MEDIUM;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterUrlParamsTest {

    @Test
    public void filterUrlParams() {
        final String obscuredPart = "<you are not supposed to read this>";
        final String url = "http://eapi.enetpulse.com/tournament_stage/participants/?includeParticipantProperties=yes&id=873678&username=sixlogicsapiusr&token=" + obscuredPart;
        final String filteredUrl = url.replaceAll(obscuredPart, "...");

        FeedRequest.FeedRequestBuilder builder = FeedRequest.newBuilder(null, MEDIUM, 3, url)
                .setUrlForLogging(filteredUrl);
        FeedRequest feedRequest = builder.build();

        assertFalse(feedRequest.toStringBasicInfo().contains(obscuredPart));
        assertTrue(feedRequest.toStringBasicInfo().contains(filteredUrl));
        assertFalse(feedRequest.toString().contains(obscuredPart));
        assertTrue(feedRequest.toString().contains(filteredUrl));

        FeedRequestWithInterval.FeedRequestWithIntervalBuilder builder2 = FeedRequestWithInterval.newBuilder(null, MEDIUM, 3, url, Duration.ZERO)
                .setUrlForLogging(filteredUrl);
        FeedRequestWithInterval feedRequestWithInterval = builder2.build();

        assertFalse(feedRequestWithInterval.toStringBasicInfo().contains(obscuredPart));
        assertTrue(feedRequestWithInterval.toStringBasicInfo().contains(filteredUrl));
        assertFalse(feedRequestWithInterval.toString().contains(obscuredPart));
        assertTrue(feedRequestWithInterval.toString().contains(filteredUrl));

    }
}
