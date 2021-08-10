package com.snapscore.pipeline.textsearch;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

public interface FullTextSearchRepository<T extends FullTextSearchableItem> {

    void addItem(T item);

    void removeItem(T item);

    void removeItemById(String itemId);

    List<T> findMatchingItems(String searchText, int maxReturnedItemsLimit, Predicate<FullTextSearchableItem> filter);

    List<T> findMatchingItems(String searchText, int maxReturnedItemsLimit, Predicate<FullTextSearchableItem> filter, Comparator<T> resultSorting);

}
