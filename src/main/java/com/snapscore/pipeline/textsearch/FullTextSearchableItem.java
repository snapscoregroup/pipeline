package com.snapscore.pipeline.textsearch;

import java.util.Collection;

public interface FullTextSearchableItem {

    /**
     * @return the name by which this item can be looked up
     */
    Collection<String> getSearchableNames();

    /**
     * @return unique item identifier by which the item can be identifier when it needs to be removed from the TrieBasedCache
     * IMPORTANT: can return null, client code needs to check !!!
     */
    String getItemIdentifier();

}
