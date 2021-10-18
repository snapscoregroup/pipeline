package com.snapscore.pipeline.textsearch;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SynonymsDictionaryImpl implements SynonymsDictionary {

    private final ConcurrentMap<String, SynonymsEntry> entriesByIdentifier = new ConcurrentHashMap<>();

    // key must be UPPER CASE
    private final ConcurrentMap<String, SynonymsEntry> entriesByPrimaryName = new ConcurrentHashMap<>();

    @Override
    public void setEntry(SynonymsEntry entry) {
        if (entry.getIdentifier() != null) {
            entriesByIdentifier.put(entry.getIdentifier(), entry);
        }
        if (entry.getPrimaryName() != null) {
            entriesByPrimaryName.put(entry.getPrimaryName().toUpperCase(), entry);
        }
    }

    @Override
    public Optional<SynonymsEntry> getEntryByName(String primaryName) {
        return Optional.ofNullable(entriesByPrimaryName.get(primaryName.toUpperCase()));
    }

    @Override
    public Optional<SynonymsEntry> getEntryByIdentifier(String identifier) {
        return Optional.ofNullable(entriesByIdentifier.get(identifier));
    }

    @Override
    public List<SynonymsEntry> getEntriesBySearchableItem(FullTextSearchableItem searchableItem) {
        final Stream<SynonymsEntry> entriesForSearchableNames = searchableItem.getSearchableNames().stream().flatMap(name -> getEntryByName(name).stream());
        final Stream<SynonymsEntry> entryForIdentifier = getEntryByIdentifier(searchableItem.getIdentifier()).stream();
        return Stream.concat(entriesForSearchableNames, entryForIdentifier).distinct().collect(Collectors.toList());
    }

}
