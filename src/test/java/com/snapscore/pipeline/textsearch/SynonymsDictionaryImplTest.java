package com.snapscore.pipeline.textsearch;

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class SynonymsDictionaryImplTest {

    @Test
    public void getEntries() {

        SynonymsDictionary synonymsDictionary = new SynonymsDictionaryImpl();

        final String identifier = "123";
        final String primaryName = "France";
        final List<String> synonyms = List.of("Fr", "Fra", "Fce");

        TestSynonymsEntry entry1 = new TestSynonymsEntry(identifier, primaryName, synonyms);

        synonymsDictionary.setEntry(entry1);

        assertEquals(Optional.of(entry1), synonymsDictionary.getEntryByIdentifier(identifier));
        assertEquals(Optional.of(entry1), synonymsDictionary.getEntryByName(primaryName));

        TestTeam testTeam = new TestTeam(identifier, primaryName);
        assertEquals(List.of(entry1), synonymsDictionary.getEntriesBySearchableItem(testTeam));
    }


}
