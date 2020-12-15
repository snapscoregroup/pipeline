package com.snapscore.pipeline.textsearch;

public class FullTextSearchConfigMock implements FullTextSearchConfig {

    @Override
    public int getMaxReturnedItemsCount() {
        return 10;
    }

}
