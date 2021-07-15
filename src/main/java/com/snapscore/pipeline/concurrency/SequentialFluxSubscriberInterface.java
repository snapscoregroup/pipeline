package com.snapscore.pipeline.concurrency;

// package private intentionally
interface SequentialFluxSubscriberInterface {

    void subscribe(Runnable onTerminateHook, Runnable onCancelHook, long itemEnqueuedTs);

}
