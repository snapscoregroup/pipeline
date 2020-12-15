package com.snapscore.pipeline.textsearch;

import java.util.List;

public interface FullTextSearchCache<T extends FullTextSearchableEntity> {

    void addItem(T item);

    void removeItem(T item);

    List<T> findMatchingItems(String searchText);

}
