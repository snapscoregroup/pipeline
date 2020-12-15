package com.snapscore.pipeline.textsearch;

import com.snapscore.pipeline.logging.Logger;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils.trim;


public class FullTextSearchCacheImpl<T extends FullTextSearchableEntity> implements FullTextSearchCache<T> {

    private static final Logger log = Logger.setup(FullTextSearchCacheImpl.class);

    private final String cacheName;

    // maps
    private final Trie<String, ConcurrentMap<String, ItemWrapper<T>>> trieMaps = new PatriciaTrie<>();

    private static final Pattern SPACE_PATTERN = Pattern.compile("\\s");
    private final int maxReturnedItemsLimit;
    private static final Comparator<String> STRING_COMPARATOR = String::compareTo;
    private static final Comparator<String> STRING_LENGTH_DESC_COMPARATOR = Comparator.comparingInt(String::length).reversed();

    private final UpdateHelper updateHelper = new UpdateHelper();
    private final QueryHelper queryHelper = new QueryHelper();
    private final StringHelper stringHelper = new StringHelper();

    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();


    /**
     * @param cacheName used for logging purposes
     */
    public FullTextSearchCacheImpl(String cacheName, int maxReturnedItemsLimit) {
        this.cacheName = cacheName;
        this.maxReturnedItemsLimit = maxReturnedItemsLimit;
    }

    /**
     * @param item item to be added
     */
    @Override
    public void addItem(T item) {
        LockingWrapper.lockAndWrite(writeLock, item1 -> updateHelper.addItem(item1), item, "Error adding item to {}; item: {}", cacheName, item);
    }

    @Override
    public void removeItem(T item) {
        LockingWrapper.lockAndWrite(writeLock, item1 -> updateHelper.removeItem(item1), item, "Error removing item from {}; item: {}", cacheName, item);
    }

    @Override
    public List<T> findMatchingItems(String searchText) {
        return LockingWrapper.lockAndGetList(readLock, () -> queryHelper.findMatchingItems(searchText),"Error finding matching items for seatchText '{}' in {}!", searchText, cacheName);
    }


    private class UpdateHelper {

        public void addItem(T item) {
            if (!dataCheckOk(item)) {
                return;
            }
            List<String> upperCaseNameKeys = stringHelper.sanitizeAndUpper(item.getSearchableNames());
            ItemWrapper<T> itemWrapper = new ItemWrapper<>(item, upperCaseNameKeys);
            List<String> itemWordKeys = itemWrapper.getItemWords();

            addToMap(itemWrapper, itemWordKeys, trieMaps);
        }

        private void addToMap(ItemWrapper<T> itemWrapper,
                              List<String> keys,
                              Map<String, ConcurrentMap<String, ItemWrapper<T>>> backingMap) {
            for (String key : keys) {
                ConcurrentMap<String, ItemWrapper<T>> matchingItems = backingMap.get(key);
                if (matchingItems == null) {
                    matchingItems = new ConcurrentHashMap<>();
                    backingMap.put(key, matchingItems);
                }
                try {
                    matchingItems.put(itemWrapper.getId(), itemWrapper);
                } catch (Exception e) {
                    log.error("Error putting item id {} to {}", itemWrapper.getId(), cacheName);
                }
            }
        }

        private void logSize(Map<String, ConcurrentMap<Integer, ItemWrapper<T>>> backingTrie) {
            int size = backingTrie.size();
            if (size % 100 == 0) {
                log.debug("Size of {} backingTrie reached {}", cacheName, size);
            }
        }

        public void removeItem(T item) {
            if (!dataCheckOk(item)) {
                return;
            }
            List<String> upperCaseNameKeys = stringHelper.sanitizeAndUpper(item.getSearchableNames());

            ItemWrapper<T> itemWrapper = new ItemWrapper<>(item, upperCaseNameKeys);
            for (String itemWordKey : itemWrapper.getItemWords()) {
                // it seems ok to remove items from the trie via this returned internal view
                SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> allPrefixMatchingItemsView = trieMaps.prefixMap(itemWordKey);
                ConcurrentMap<String, ItemWrapper<T>> singleKeyItemsMap = allPrefixMatchingItemsView.get(itemWordKey);
                if (singleKeyItemsMap != null) {
                    singleKeyItemsMap.remove(item.getId()); // java.lang.NullPointerException: null
                    if (singleKeyItemsMap.isEmpty()) {
                        trieMaps.remove(itemWordKey);
                    } else {
                        // do not remove, other items still mapped to this prefix word
                    }
                }
            }

        }

        private boolean dataCheckOk(T item) {
            if (item == null) {
                log.warn("{} Cannot process null item; item: {}", cacheName, item);
                return false;
            }
            if (item.getId() == null) {
                log.warn("{} Cannot process item with null id; item: {}", cacheName, item);
                return false;
            }
            if (item.getSearchableNames().isEmpty()) {
                log.warn("{} Cannot process item with empty names; item: {}", cacheName, item);
                return false;
            }
            return true;
        }

    }


    private class QueryHelper {

        public List<T> findMatchingItems(String searchText) {
            if (!isValid(searchText)) {
                return Collections.EMPTY_LIST;
            } else {
                String searchTextSanitized = stringHelper.sanitizeAndUpper(searchText);
                return doApproximateSearch(searchText, searchTextSanitized);
            }
        }

        private List<T> doApproximateSearch(String searchText, String searchTextSanitized) {
            boolean hasMultipleWords = stringHelper.containsSpaces(searchTextSanitized);
            if (hasMultipleWords) {
                List<String> searchTextWords = stringHelper.splitToMutableList(searchTextSanitized);
                if (!isValid(searchTextWords)) {
                    return Collections.EMPTY_LIST;
                }
                searchTextWords.sort(STRING_LENGTH_DESC_COMPARATOR); // big words first as they might get fewer matches than very short words

                int smallestMatchingMapSize = -1;
                String smallestMatchingMapWord = null;
                SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap = null;
                for (String searchTextWord : searchTextWords) {
                    SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> currPrefixMap = trieMaps.prefixMap(searchTextWord);
                    int currMapSize = currPrefixMap.size();
                    if (currMapSize == 0) {
                        return Collections.EMPTY_LIST; // one of input words does not match -> return nothing ... user needs to correct input
                    }
                    if (smallestMatchingMapSize > -1) {
                        if (currMapSize < smallestMatchingMapSize) {
                            smallestMatchingMapSize = currMapSize;
                            smallestMatchingMap = currPrefixMap;
                            smallestMatchingMapWord = searchTextWord;
                        }
                    } else {
                        smallestMatchingMapSize = currMapSize;
                        smallestMatchingMap = currPrefixMap;
                        smallestMatchingMapWord = searchTextWord;
                    }
                    if (smallestMatchingMapSize < 5) {
                        // performance shortcut - if current result is small enough we need not go on, this match is good enough ...
                        break;
                    }
                }

                if (smallestMatchingMap != null) {
                    searchTextWords.remove(smallestMatchingMapWord); // this one is already matched as it provided the prefixMap > remove from list
                    searchTextWords.sort(STRING_COMPARATOR);
                    List<T> matchingItems = smallestMatchingMap.values().stream()
                            .flatMap(stagesMap -> stagesMap.values().stream())
                            .distinct()
                            .filter(itemWrapper -> itemWrapper.matchesAll(searchTextWords))
                            .limit(maxReturnedItemsLimit)
                            .map(ItemWrapper::getItem)
                            .collect(Collectors.toList());
                    return matchingItems;
                } else {
                    log.debug("{} No items found!", cacheName);
                    return Collections.EMPTY_LIST;
                }

            } else {
                SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> prefixMap = trieMaps.prefixMap(searchTextSanitized);
                logPrefixMapInfo(searchText, prefixMap);
                List<T> foundForSubstr = prefixMap.values().stream()
                        .flatMap(stagesMap -> stagesMap.values().stream())
                        .distinct()
                        .limit(maxReturnedItemsLimit)
                        .map(ItemWrapper::getItem)
                        .collect(Collectors.toList());
                return foundForSubstr;
            }
        }

        private void logPrefixMapInfo(String searchTextWord, SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> currPrefixMap) {
            log.debug("PrefixMap keys size = {}; total values = {} for key {}", currPrefixMap.keySet().size(), currPrefixMap.values().stream().mapToLong(m -> m.values().size()).sum(), searchTextWord);
        }

        private boolean isValid(String searchText) {
            return searchText != null;
        }

        private boolean isValid(List<String> searchTextWords) {
            return !searchTextWords.isEmpty() && searchTextWords.size() < 15;
        }

    }


    private static class StringHelper {

        private boolean containsSpaces(String searchTextSanitized) {
            return SPACE_PATTERN.matcher(searchTextSanitized).find();
        }

        private String sanitizeAndUpper(String keyBase) {
            return trim(keyBase).toUpperCase();
        }

        private List<String> sanitizeAndUpper(List<String> keyBases) {
            return keyBases.stream()
                    .filter(keyBase -> keyBase != null)
                    .map(keyBase -> keyBase.trim().toUpperCase())
                    .collect(Collectors.toList());
        }

        private List<String> splitToMutableList(String searchTextSanitized) {
            String[] words = SPACE_PATTERN.split(searchTextSanitized);
            // we need a mutable list so we do not want to use Arrays.asList()
            if (words.length > 0) {
                List<String> wordsList = new ArrayList<>(words.length);
                wordsList.addAll(Arrays.asList(words));
                return wordsList;
            } else {
                return Collections.EMPTY_LIST;
            }
        }
    }


    /**
     * Encapsulates item data in a format suited for better lookup performance
     */
    private static class ItemWrapper<T extends FullTextSearchableEntity> implements FullTextSearchableEntity {

        // upper case name of the stored item split into words
        private final T item;
        private final List<String> itemWords = new ArrayList<>();
        private List<String> searchableNames;

        public ItemWrapper(T item, List<String> upperCaseNames) {
            this.item = item;
            this.searchableNames = item.getSearchableNames();
            for (String upperCaseName : upperCaseNames) {
                String[] itemWordsArr = SPACE_PATTERN.split(upperCaseName);
                Arrays.stream(itemWordsArr)
                        .map(word -> word.intern()) // intern all words as there are many repetitions
                        .sorted(STRING_COMPARATOR) // needs to be sorted for better search performance
                        .forEach(itemWords::add);
            }
        }

        boolean matchesAll(List<String> searchTextWords) {
            for (String searchTextWord : searchTextWords) {
                boolean matches = false;
                for (int idx = 0; idx < itemWords.size(); idx++) {
                    String itemWord = itemWords.get(idx);
                    if (itemWord.length() >= searchTextWord.length()) {
                        int charDiff = itemWord.charAt(0) - searchTextWord.charAt(0);
                        if (charDiff > 0) {
                            // no following itemWords can match this searchTextWords - can skip the rest
                            return false;
                        } else if (charDiff == 0) {
                            matches = itemWord.startsWith(searchTextWord);
                            if (matches) {
                                break;
                            }
                        } else {
                            // continue
                        }
                    }
                }
                if (!matches) { // if any one search word does not match anything from this item -> quit
                    return false;
                }
            }
            return true; // all passed
        }

        public List<String> getItemWords() {
            return Collections.unmodifiableList(itemWords);
        }

        public T getItem() {
            return item;
        }

        @Override
        public List<String> getSearchableNames() {
            return this.searchableNames;
        }

        @Override
        public String getId() {
            return item.getId();
        }

        @Override
        public String toString() {
            return "ItemWrapper{" +
                    "item=" + item +
                    ", itemWords=" + itemWords +
                    ", searchableNames=" + searchableNames +
                    '}';
        }
    }

}
