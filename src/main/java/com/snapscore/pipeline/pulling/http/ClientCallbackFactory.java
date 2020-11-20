package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.FeedRequest;
import reactor.core.publisher.MonoSink;

public interface ClientCallbackFactory<T> {

    T createCallback(FeedRequest feedRequest, MonoSink<byte[]> emitter);

}
