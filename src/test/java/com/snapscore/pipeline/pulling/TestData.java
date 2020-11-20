package com.snapscore.pipeline.pulling;

public class TestData {

    public static final FeedName STAGE_FIXTURES_FEED_NAME = new FeedName(TestData.FeedNameEnum.STAGE_FIXTURES_FEED);
    public static final FeedName MATCH_DETAIL_FEED_NAME = new FeedName(TestData.FeedNameEnum.MATCH_DETAIL_FEED);

    public enum FeedNameEnum {
        MATCH_DETAIL_FEED,
        STAGE_DETAIL_FEED,
        STAGE_FIXTURES_FEED
    }

    public enum FeedDataTypeEnum {
        MATCH_DETAIL,
        STAGE_DETAIL,
        STAGE_FIXTURES
    }
}
