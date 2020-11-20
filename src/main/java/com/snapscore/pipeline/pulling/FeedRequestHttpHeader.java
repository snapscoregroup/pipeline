package com.snapscore.pipeline.pulling;


public class FeedRequestHttpHeader {

    private final String key;
    private final String value;

    public FeedRequestHttpHeader(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "FeedRequestHttpHeader{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
