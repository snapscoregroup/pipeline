package com.snapscore.pipeline.pulling.http;

import java.time.Duration;

public class VertxHttpClientConfigImpl implements HttpClientConfig {

    private final Duration readTimeout;
    private final String host;
    private final int port;

    public VertxHttpClientConfigImpl(Duration readTimeout, String host, int port) {
        this.readTimeout = readTimeout;
        this.host = host;
        this.port = port;
    }

    @Override
    public int getNumberOfThreads() {
        throw new UnsupportedOperationException("This operation is not supported for VertxHttpClient");
    }

    @Override
    public Duration readTimeout() {
        return readTimeout;
    }

    @Override
    public String host() {
        return host;
    }

    @Override
    public int port() {
        return port;
    }

}
