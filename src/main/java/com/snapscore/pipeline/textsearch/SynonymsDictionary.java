package com.snapscore.pipeline.textsearch;

import java.util.List;
import java.util.Optional;

public interface SynonymsDictionary {

    void setEntry(SynonymsEntry entry);

    Optional<SynonymsEntry> getEntryByName(String primaryName);

    Optional<SynonymsEntry> getEntryByIdentifier(String identifier);

    List<SynonymsEntry> getEntriesBySearchableItem(FullTextSearchableItem searchableItem);

}
