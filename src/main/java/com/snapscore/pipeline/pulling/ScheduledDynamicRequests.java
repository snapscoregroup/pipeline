package com.snapscore.pipeline.pulling;

import reactor.core.Disposable;

import java.time.Duration;

/**
 * Represents requests that are scheduled as a group and created dynamically at a given interval when every time before they are pulled.
 * Used e.g. for situation where the URL of a request needs to be created from scratch every time
 * as it contains a dynamic parameter such as a date or timestamp
 */
public class ScheduledDynamicRequests<K> extends ScheduledPulling<K> {

    public ScheduledDynamicRequests(K scheduledPullingKey, Disposable disposable, Duration pullInterval) {
        super(scheduledPullingKey, disposable, pullInterval);
    }

}
