package com.snapscore.pipeline.pulling.http;

public interface ClientCallback {

    // implementations must emit the received data
    void handleSuccessfulResponse(byte[] responseData);

    // implementations must emit FailedRequestException
    void handleUnsuccessfulResponse(int statusCode);

    // implementations must emit FailedRequestException
    void handleException(Throwable e);

    void emitResponseData(byte[] responseData);

    void emitFailedRequestException();

}
