package com.snapscore.pipeline.pulling.http;

import io.vertx.core.http.HttpClientResponse;

public interface VertxClientCallback extends ClientCallback {

    void onResponse(HttpClientResponse response);

}
