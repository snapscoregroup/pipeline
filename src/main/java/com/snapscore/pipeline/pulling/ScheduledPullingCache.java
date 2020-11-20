package com.snapscore.pipeline.pulling;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Used to track specific scheduledPulling instances mapped to a specific key.
 * Tracking of all scheduledPullings is good for pulling cancellation when the feed url pulling is not relevant
 * any more or when we want to change some of its attributes - such as pulling frequency
 *
 * @param <K> key - must provide and implementation of equals() and hashCode() methods
 * @param <V> scheduledPulling value
 */
public interface ScheduledPullingCache<K, V extends ScheduledPulling<K>> {

    /**
     * Adds the specified scheduledPulling mapped to scheduledPullingKey in the cache.
     * If a previous mapping to the same scheduledPullingKey exists then it is cancelled and replaced with the specified scheduledPulling
     *
     * @param scheduledPulling
     */
    void setScheduledPulling(V scheduledPulling);

    Optional<V> getScheduledPulling(K scheduledPullingKey);

    /**
     * Removes the scheduledPuling from the cache but DOES NOT CANCEL it!
     * @param scheduledPullingKey
     */
    Optional<V> removeScheduledPulling(K scheduledPullingKey);

    /**
     * Cancels the scheduledPuling for the given key and removes it from the cache
     * @param scheduledPullingKey
     */
    void cancelScheduledPulling(K scheduledPullingKey);

    /**
     * Cancels and removes all scheduledPullings complying with the given condition
     * @param cancellationCondition
     * @return a list of all scheduledPullingKey instances whose mapped pulling was cancelled and removed from the cache
     */
    List<K> cancelAllScheduledPulling(Predicate<K> cancellationCondition);

    void cancelAllScheduledPulling();

    boolean isPullingScheduledFor(K scheduledPullingKey);

    int size();

}
