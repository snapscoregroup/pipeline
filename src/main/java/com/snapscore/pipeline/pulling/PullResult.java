package com.snapscore.pipeline.pulling;

import java.util.Arrays;
import java.util.Objects;

public class PullResult {

    private final FeedRequest feedRequest;
    private final byte[] data;

    public PullResult(FeedRequest feedRequest, byte[] data) {
        this.feedRequest = feedRequest;
        this.data = data;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PullResult)) return false;
        PullResult that = (PullResult) o;
        return Objects.equals(feedRequest, that.feedRequest) &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(feedRequest);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "PullResult{" +
                "feedRequest=" + feedRequest +
                ", data=" + data +
                '}';
    }
}
