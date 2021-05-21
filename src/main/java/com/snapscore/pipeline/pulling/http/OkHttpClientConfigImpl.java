package com.snapscore.pipeline.pulling.http;

import java.time.Duration;

public class OkHttpClientConfigImpl implements HttpClientConfig {

    private final int numberOfThreads;
    private final Duration readTimeout;

    public OkHttpClientConfigImpl(int numberOfThreads, Duration readTimeout) {
        this.numberOfThreads = numberOfThreads;
        this.readTimeout = readTimeout;
    }

    @Override
    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    @Override
    public Duration readTimeout() {
        return readTimeout;
    }

    @Override
    public String host() {
        throw new UnsupportedOperationException("This operation is not supported for OkHttpClient");
    }

    @Override
    public int port() {
        throw new UnsupportedOperationException("This operation is not supported for OkHttpClient");
    }
}
