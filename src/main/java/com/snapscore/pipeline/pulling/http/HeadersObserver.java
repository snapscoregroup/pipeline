package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedName;
import io.vertx.core.MultiMap;

public interface HeadersObserver {

    void observeHeaders(FeedName feedName, MultiMap headers);

}
