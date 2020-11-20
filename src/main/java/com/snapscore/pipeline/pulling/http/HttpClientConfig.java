package com.snapscore.pipeline.pulling.http;

import java.time.Duration;

public interface HttpClientConfig {

    int getNumberOfThreads();

    Duration readTimeout();

    String host();

    int port();

}
