package com.snapscore.pipeline.pulling.http;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

import java.io.IOException;

public interface OkHttpClientCallback extends Callback, ClientCallback {

    // implementations must make sure that before method is exited either successfully received data is emitted
    // or FailedRequestException is emitted. This is crucial for scheduling and retry logic.
    void onResponse(Call call, Response response);

    // implementations must make sure that before method is exited FailedRequestException is emitted. This is crucial for scheduling and retry logic.
    void onFailure(Call call, IOException e);

}
