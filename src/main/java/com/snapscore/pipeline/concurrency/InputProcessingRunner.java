package com.snapscore.pipeline.concurrency;

public abstract class InputProcessingRunner<I, R> {

    /**
     * Intentionally protected visibility
     * This is only meant to be used internally by the library
     */
    protected abstract void run(Runnable onTerminateHook, Runnable onCancelHook, long itemEnqueuedTs);

}
