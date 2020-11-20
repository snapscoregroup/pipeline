package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;
import java.util.concurrent.CompletableFuture;

public interface HttpClient {

    CompletableFuture<byte[]> getAsync(FeedRequest feedRequest);

    void shutdown();

}
