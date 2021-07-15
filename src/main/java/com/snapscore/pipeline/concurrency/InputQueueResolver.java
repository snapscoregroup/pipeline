package com.snapscore.pipeline.concurrency;

public abstract class InputQueueResolver<I> {

    /**
     * @param inputQueueCount total count of input queues provided by the queuing component
     * @see ConcurrentSequentialProcessorImpl
     */
    public abstract int getQueueIdxFor(I input, int inputQueueCount);

    protected int calcIdx(int inputQueueCount, String inputEntityIdentifier) {
        return Math.abs(inputEntityIdentifier.hashCode()) % inputQueueCount;
    }

    protected int calcIdx(int inputQueueCount, int inputEntityIdentifier) {
        return inputEntityIdentifier % inputQueueCount;
    }


}
