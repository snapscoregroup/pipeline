package com.snapscore.pipeline.utils.reactive.sequentialisation;

public interface SequentialFluxProcessor {

    <I, R> void processSequentially(SequentialInput<I, R> sequentialInput);

}
