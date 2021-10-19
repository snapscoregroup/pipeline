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
        final String name = "France";
        final List<String> synonyms = List.of(name, "Fr", "Fra", "Fce");

        TestSynonymsEntry entry1 = new TestSynonymsEntry(identifier, synonyms);

        synonymsDictionary.setEntry(entry1);

        assertEquals(Optional.of(entry1), synonymsDictionary.getEntryByIdentifier(identifier));
        assertEquals(Optional.of(entry1), synonymsDictionary.getEntryByName("Fce"));

        TestTeam testTeam = new TestTeam(identifier, name);
        assertEquals(List.of(entry1), synonymsDictionary.getEntriesBySearchableItem(testTeam));
    }


}
