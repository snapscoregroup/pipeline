package com.snapscore.pipeline.textsearch;

import com.snapscore.pipeline.logging.Logger;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils.trim;


public class FullTextSearchRepositoryImpl<T extends FullTextSearchableItem> implements FullTextSearchRepository<T> {

    private static final Logger log = Logger.setup(FullTextSearchRepositoryImpl.class);

    private final String cacheName;

    // maps
    private final Trie<String, ConcurrentMap<String, ItemWrapper<T>>> trieMaps = new PatriciaTrie<>();

    // only as a helper map to track by id what we have stored here ...
    private final ConcurrentMap<String, T> itemsByIdHelperMap = new ConcurrentHashMap<>();

    private static final Pattern SPACE_PATTERN = Pattern.compile("\\s");
    private static final Comparator<String> STRING_COMPARATOR = String::compareTo;
    private static final Comparator<String> STRING_LENGTH_DESC_COMPARATOR = Comparator.comparingInt(String::length).reversed();

    private final UpdateHelper updateHelper = new UpdateHelper();
    private final QueryHelper queryHelper = new QueryHelper();
    private final StringHelper stringHelper = new StringHelper();

    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(false); // false - important for performance reasons
    private final ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();


    /**
     * @param cacheName used for logging purposes
     */
    public FullTextSearchRepositoryImpl(String cacheName) {
        this.cacheName = cacheName;
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
    public void removeItemById(String itemId) {
        LockingWrapper.lockAndWrite(writeLock, itemId0 -> updateHelper.removeItemById(itemId), itemId, "Error removing item from {}; itemId: {}", cacheName, itemId);
    }

    @Override
    public List<T> findMatchingItems(String searchText, int maxReturnedItemsLimit) {
        return LockingWrapper.lockAndGetList(readLock, () -> queryHelper.findMatchingItems(searchText, maxReturnedItemsLimit, null), "Error finding matching items for seatchText '{}' in {}!", searchText, cacheName);
    }

    public List<T> findMatchingItemss(String searchText, int maxReturnedItemsLimit, Predicate<FullTextSearchableItem> predicate) {
        return LockingWrapper.lockAndGetList(readLock, () -> queryHelper.findMatchingItems(searchText, maxReturnedItemsLimit, predicate), "Error finding matching items for seatchText '{}' in {}!", searchText, cacheName);
    }


    private class UpdateHelper {

        public void addItem(T item) {
            if (!dataCheckOk(item)) {
                return;
            } else {
                addToMaps(item);
            }
        }

        private void addToMaps(T item) {
            removePreviousVersionFromTrieMaps(item);

            itemsByIdHelperMap.put(item.getIdentifier(), item);

            List<String> upperCaseNameKeys = stringHelper.sanitizeAndUpper(item.getSearchableNames());
            ItemWrapper<T> itemWrapper = new ItemWrapper<>(item, upperCaseNameKeys);
            List<String> itemWordKeys = itemWrapper.getItemWords();

            for (String key : itemWordKeys) {
                ConcurrentMap<String, ItemWrapper<T>> matchingItems = trieMaps.get(key);
                if (matchingItems == null) {
                    matchingItems = new ConcurrentHashMap<>();
                    trieMaps.put(key, matchingItems);
                }
                try {
                    matchingItems.put(itemWrapper.getIdentifier(), itemWrapper);
                } catch (Exception e) {
                    log.error("Error putting item id {} to {}", itemWrapper.getIdentifier(), cacheName);
                }
            }
        }

        private void removePreviousVersionFromTrieMaps(T item) {
            boolean alreadyExists = itemsByIdHelperMap.containsKey(item.getIdentifier());
            if (alreadyExists) {
                removeItemById(item.getIdentifier());
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
                    singleKeyItemsMap.remove(item.getIdentifier()); // java.lang.NullPointerException: null
                    if (singleKeyItemsMap.isEmpty()) {
                        trieMaps.remove(itemWordKey);
                    } else {
                        // do not remove, other items still mapped to this prefix word
                    }
                }
            }
        }

        public void removeItemById(String itemId) {
            if (itemId != null) {
                T itemToRemove = itemsByIdHelperMap.get(itemId);
                if (itemToRemove != null) {
                    removeItem(itemToRemove);
                }
            }
        }

        private boolean dataCheckOk(T item) {
            if (item == null) {
                log.warn("{} Cannot process null item; item: {}", cacheName, item);
                return false;
            }
            if (item.getIdentifier() == null) {
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

        public List<T> findMatchingItems(String searchText, int returnedItemsLimit, Predicate<FullTextSearchableItem> predicate) {
            if (!isValid(searchText)) {
                return Collections.EMPTY_LIST;
            } else {
                String searchTextSanitized = stringHelper.sanitizeAndUpper(searchText);
                return search(searchText, searchTextSanitized, returnedItemsLimit, predicate);
            }
        }

        private List<T> search(String searchText, String searchTextSanitized, int returnedItemsLimit, Predicate<FullTextSearchableItem> predicate) {
            boolean hasMultipleWords = stringHelper.containsSpaces(searchTextSanitized);
            if (hasMultipleWords) {
                return searchByMultipleWordInput(searchTextSanitized, returnedItemsLimit, predicate);
            } else {
                return doSearchBySingleWordInput(searchText, searchTextSanitized, returnedItemsLimit, predicate);
            }
        }

        private List searchByMultipleWordInput(String searchTextSanitized, int returnedItemsLimit, Predicate<FullTextSearchableItem> predicate) {
            List<String> searchTextWords = stringHelper.splitToMutableList(searchTextSanitized);
            if (!isValid(searchTextWords)) {
                return Collections.EMPTY_LIST;
            } else {
                SmallestMatch<T> smallestMatch = findSmallestMatch(searchTextWords);
                if (smallestMatch.smallestMatchingMap != null) {
                    return getResultForMultiWordSearch(returnedItemsLimit, searchTextWords,
                            smallestMatch.smallestMatchingMapWord, smallestMatch.smallestMatchingMap, predicate);
                } else {
                    log.debug("{} No items found!", cacheName);
                    return Collections.EMPTY_LIST;
                }
            }
        }

        private SmallestMatch<T> findSmallestMatch(List<String> searchTextWords) {
            searchTextWords.sort(STRING_LENGTH_DESC_COMPARATOR); // big words first as they might get fewer matches than very short words ... good for faster search
            int smallestMatchingMapSize = -1;
            String smallestMatchingMapWord = null;
            SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap = null;

            for (String searchTextWord : searchTextWords) {
                SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> currPrefixMap = trieMaps.prefixMap(searchTextWord);
                int currMapSize = currPrefixMap.size();
                if (currMapSize == 0) {
                    break; // one of input words does not match -> return nothing ... user needs to correct input
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

            return new SmallestMatch(smallestMatchingMapWord, smallestMatchingMap);
        }

        private List<T> getResultForMultiWordSearch(int returnedItemsLimit,
                                                    List<String> searchTextWords,
                                                    String smallestMatchingMapWord,
                                                    SortedMap<String, ConcurrentMap<String,
                                                            ItemWrapper<T>>> smallestMatchingMap,
                                                    Predicate<FullTextSearchableItem> predicate
        ) {
            searchTextWords.remove(smallestMatchingMapWord); // this one is already matched as it provided the prefixMap > remove from list
            searchTextWords.sort(STRING_COMPARATOR); // IMPORTANT to be sorted for the rest of the search algorithm to work correctly and to perform best
            return smallestMatchingMap.values().stream()
                    .flatMap(stagesMap -> stagesMap.values().stream())
                    .distinct()
                    .filter(predicate)
                    .filter(itemWrapper -> itemWrapper.matchesAll(searchTextWords))
                    .limit(returnedItemsLimit)
                    .map(ItemWrapper::getItem)
                    .collect(Collectors.toList());
        }

        private List<T> doSearchBySingleWordInput(String searchText, String searchTextSanitized, int maxReturnedItemsLimit,
                                                  Predicate<FullTextSearchableItem> predicate) {
            SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> prefixMap = trieMaps.prefixMap(searchTextSanitized);
            return getResultForSingleWordSearch(searchText, maxReturnedItemsLimit, prefixMap, predicate);
        }

        private List<T> getResultForSingleWordSearch(String searchText,
                                                     int maxReturnedItemsLimit,
                                                     SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> prefixMap,
                                                     Predicate<FullTextSearchableItem> predicate) {
            logPrefixMapInfo(searchText, prefixMap);
            return prefixMap.values().stream()
                    .flatMap(stagesMap -> stagesMap.values().stream())
                    .distinct()
                    .limit(maxReturnedItemsLimit)
                    .filter(i -> predicate.test(i))
                    .map(ItemWrapper::getItem)
                    .collect(Collectors.toList());
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

    private static class SmallestMatch<T extends FullTextSearchableItem> {
        private final String smallestMatchingMapWord;
        private final SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap;

        public SmallestMatch(String smallestMatchingMapWord,
                             SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap) {
            this.smallestMatchingMapWord = smallestMatchingMapWord;
            this.smallestMatchingMap = smallestMatchingMap;
        }
    }


    private static class StringHelper {

        private boolean containsSpaces(String searchTextSanitized) {
            return SPACE_PATTERN.matcher(searchTextSanitized).find();
        }

        private String sanitizeAndUpper(String keyBase) {
            return trim(keyBase).toUpperCase();
        }

        private List<String> sanitizeAndUpper(Collection<String> keyBases) {
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
    private static class ItemWrapper<T extends FullTextSearchableItem> implements FullTextSearchableItem {

        // upper case name of the stored item split into words
        private final T item;
        private final List<String> itemWords;
        private Collection<String> searchableNames;

        public ItemWrapper(T item, List<String> upperCaseNames) {
            this.item = item;
            this.searchableNames = item.getSearchableNames();
            this.itemWords = upperCaseNames.stream()
                    .flatMap(upperCaseName -> Arrays.stream(SPACE_PATTERN.split(upperCaseName)))
                    .sorted(STRING_COMPARATOR) // IMPORTANT! needs to be sorted for better search performance; the search algorithm relies on the being sorted!
                    .map(word -> word.intern()) // intern all words as there are many repetitions // TODO make interning optional
                    .collect(Collectors.toList());
        }

        boolean matchesAll(List<String> searchTextWordsSortedByNaturalOrder) {
            for (String searchTextWord : searchTextWordsSortedByNaturalOrder) {
                boolean matches = false;
                for (int idx = 0; idx < itemWords.size(); idx++) {
                    String itemWord = itemWords.get(idx);
                    if (itemWord.length() >= searchTextWord.length()) {
                        int charDiff = itemWord.charAt(0) - searchTextWord.charAt(0);
                        if (charDiff > 0) {
                            // no following itemWords can match this searchTextWordsSortedByNaturalOrder - can skip the rest
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
        public Collection<String> getSearchableNames() {
            return this.searchableNames;
        }

        @Override
        public String getIdentifier() {
            return item.getIdentifier();
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
