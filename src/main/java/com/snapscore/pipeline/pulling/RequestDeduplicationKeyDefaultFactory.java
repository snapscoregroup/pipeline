package com.snapscore.pipeline.pulling;

import java.util.function.Function;

public class RequestDeduplicationKeyDefaultFactory implements Function<FeedRequest, String> {

    @Override
    public String apply(FeedRequest feedRequest) {
        // priority needs to be taken into account as well so that two requests to the same URL both go through
        // If the second incoming request has higher priority it must not considered a duplicate and thus ignored
        return feedRequest.getUrl() + "_" + feedRequest.getPriority().name();
    }

}
