package com.snapscore.pipeline.textsearch;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FullTextSearchRepositoryImplTest {

    private TestTeam team1;
    private TestTeam team2;
    private TestTeam team3;
    private TestTeam team4;
    private TestTeam team5;

    @Before
    public void setUp() throws Exception {
        team1 = new TestTeam("1", "Alfa");
        team2 = new TestTeam("2", "Alb");
        team3 = new TestTeam("3", "American team");
        team4 = new TestTeam("4", "American tornado");
        team5 = new TestTeam("5", "America sucks");
    }


    @Test
    public void addItem() {
        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestTrieCache", 100);
        trieCache.addItem(team1);
        trieCache.addItem(team2);
        trieCache.addItem(team3);

        List<TestTeam> matchingItems = trieCache.findMatchingItems("Al");
        matchingItems.sort(Comparator.comparing(TestTeam::getItemIdentifier));
        assertEquals(List.of(team1, team2), matchingItems);
    }


    @Test
    public void removeItem() {
        // given
        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestTrieCache", 100);
        trieCache.addItem(team1);
        trieCache.addItem(team2);
        trieCache.addItem(team3);
        trieCache.addItem(team4);
        trieCache.addItem(team5);

        trieCache.removeItem(team2);

        List<TestTeam> matchingItems = trieCache.findMatchingItems("American");
        assertEquals(List.of(team3, team4), matchingItems);

        // when
        trieCache.removeItem(team3);

        // then
        matchingItems = trieCache.findMatchingItems("American");
        assertEquals(List.of(team4), matchingItems);

        matchingItems = trieCache.findMatchingItems("team");
        assertEquals(Collections.EMPTY_LIST, matchingItems);

        matchingItems = trieCache.findMatchingItems("America");
        assertEquals(List.of(team5, team4), matchingItems);
    }

    @Test
    public void findMatchingItemsForMultiWordInput() {

        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestTrieCache", 100);
        trieCache.addItem(team1);
        trieCache.addItem(team2);
        trieCache.addItem(team3);
        trieCache.addItem(team4);
        trieCache.addItem(team5);

        List<TestTeam> matchingItems = trieCache.findMatchingItems("Ame te");
        assertEquals(List.of(team3), matchingItems);

        matchingItems = trieCache.findMatchingItems("Ame t");
        assertEquals(List.of(team3, team4), matchingItems);
    }

    private static class TestTeam implements FullTextSearchableItem {

        private final String id;
        private final List<String> names;

        public TestTeam(String id, String name) {
            this.id = id;
            this.names = List.of(name);
        }


        public TestTeam(String id, List<String> names) {
            this.id = id;
            this.names = names;
        }

        @Override
        public List<String> getSearchableNames() {
            return names;
        }

        @Override
        public String getItemIdentifier() {
            return id;
        }
    }

}
