package com.snapscore.pipeline.textsearch;

import java.util.List;
import java.util.Optional;

public interface SynonymsDictionary {

    void setEntry(SynonymsEntry entry);

    /**
     * @param name  One of the synonym names
     */
    Optional<SynonymsEntry> getEntryByName(String name);

    Optional<SynonymsEntry> getEntryByIdentifier(String identifier);

    List<SynonymsEntry> getEntriesBySearchableItem(FullTextSearchableItem searchableItem);

}
