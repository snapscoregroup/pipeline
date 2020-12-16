package com.snapscore.pipeline.textsearch;

import java.util.List;

public interface FullTextSearchRepository<T extends FullTextSearchableItem> {

    void addItem(T item);

    void removeItem(T item);

    List<T> findMatchingItems(String searchText);

}
