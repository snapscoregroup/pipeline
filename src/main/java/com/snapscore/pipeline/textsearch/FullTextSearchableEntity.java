package com.snapscore.pipeline.textsearch;

import java.util.List;

public interface FullTextSearchableEntity {

    /**
     * @return the name by which this entity can be looked up
     */
    List<String> getSearchableNames();

    /**
     * @return unique entity identifier by which the entoty can be identifier when it needs to be removed from the TrieBasedCache
     * IMPORTANT: can return null, client code needs to check !!!
     */
    String getSearchableId();

}
