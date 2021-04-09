package com.snapscore.pipeline.utils.reactive.sequentialisation;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.snapscore.pipeline.utils.reactive.sequentialisation.TestSupport.*;
import static org.junit.Assert.*;

public class SequentialFluxProcessorCompletionAwaiterTest {

    public static final int HEAVY_PROCESSING_MILLIS = 0;

    @Test
    public void emptyProcessorAreCompetedWithoutTimeout() {

        SequentialFluxProcessor sequentialFluxProcessor1 = new SequentialFluxProcessorImpl("test-sequential-processor-1");
        SequentialFluxProcessor sequentialFluxProcessor2 = new SequentialFluxProcessorImpl("test-sequential-processor-2");

        try {
            SequentialFluxProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(sequentialFluxProcessor1, sequentialFluxProcessor2), Duration.ofMillis(1000));
        } catch (Exception e) {
            fail("awaiting processing completion should not have timed out");
        }
    }


    @Test
    public void independentProcessorsAreCompletedWithinTimeoutLimit() {

        SequentialFluxProcessor sequentialFluxProcessor1 = new SequentialFluxProcessorImpl("test-sequential-processor-1");
        SequentialFluxProcessor sequentialFluxProcessor2 = new SequentialFluxProcessorImpl("test-sequential-processor-2");

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, this::processTestMessageFlux);
        sequentialMessages.forEach(sequentialFluxProcessor1::processSequentiallyAsync);
        sequentialMessages.forEach(sequentialFluxProcessor2::processSequentiallyAsync);

        try {
            SequentialFluxProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(sequentialFluxProcessor1, sequentialFluxProcessor2), Duration.ofMillis(2000));
        } catch (Exception e) {
            fail("awaiting processing completion should not have timed out");
        }

        assertEquals(0L, sequentialFluxProcessor1.getTotalUnprocessedInputs());
        assertEquals(0L, sequentialFluxProcessor2.getTotalUnprocessedInputs());
    }

    @Test(expected = TimeoutException.class)
    public void independentProcessorsAreTimedOut() throws Exception {

        SequentialFluxProcessor sequentialFluxProcessor1 = new SequentialFluxProcessorImpl("test-sequential-processor-1");
        SequentialFluxProcessor sequentialFluxProcessor2 = new SequentialFluxProcessorImpl("test-sequential-processor-2");

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, this::processTestMessageFlux);
        sequentialMessages.forEach(sequentialFluxProcessor1::processSequentiallyAsync);
        sequentialMessages.forEach(sequentialFluxProcessor2::processSequentiallyAsync);

        SequentialFluxProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(sequentialFluxProcessor1, sequentialFluxProcessor2), Duration.ofMillis(10));
    }


    @Test
    public void interdependentProcessorsAreCompletedWithinTimeoutLimit() {

        SequentialFluxProcessor sequentialFluxProcessor1 = new SequentialFluxProcessorImpl("test-sequential-processor-1");
        SequentialFluxProcessor sequentialFluxProcessor2 = new SequentialFluxProcessorImpl("test-sequential-processor-2");

        final Function<TestMessage, Flux<TestMessage>> processingForProcessor1 = testMessage1 -> {
            return Flux.just(testMessage1)
                    .doOnNext(testMessage -> {
                        // processor one will forward all processing to processor 2
                        sequentialFluxProcessor2.processSequentiallyAsync(createSequentialInput(this::processTestMessageFlux, testMessage.entityId, testMessage));
                    });
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, processingForProcessor1);
        sequentialMessages.forEach(sequentialFluxProcessor1::processSequentiallyAsync);

        try {
            SequentialFluxProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(sequentialFluxProcessor1, sequentialFluxProcessor2), Duration.ofMillis(2000));
        } catch (Exception e) {
            fail("awaiting processing completion should not have timed out");
        }

        assertEquals(0L, sequentialFluxProcessor1.getTotalUnprocessedInputs());
        assertEquals(0L, sequentialFluxProcessor2.getTotalUnprocessedInputs());
    }


    @Test(expected = TimeoutException.class)
    public void interdependentProcessorsAreTimedOut() throws Exception {

        SequentialFluxProcessor sequentialFluxProcessor1 = new SequentialFluxProcessorImpl("test-sequential-processor-1");
        SequentialFluxProcessor sequentialFluxProcessor2 = new SequentialFluxProcessorImpl("test-sequential-processor-2");

        final Function<TestMessage, Flux<TestMessage>> processingForProcessor1 = testMessage1 -> {
            return Flux.just(testMessage1)
                    .doOnNext(testMessage -> {
                        // processor one will forward all processing to processor 2 ... making the processing dependent
                        sequentialFluxProcessor2.processSequentiallyAsync(createSequentialInput(this::processTestMessageFlux, testMessage.entityId, testMessage));
                    });
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, processingForProcessor1);
        sequentialMessages.forEach(sequentialFluxProcessor1::processSequentiallyAsync);

        SequentialFluxProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(sequentialFluxProcessor1, sequentialFluxProcessor2), Duration.ofMillis(10));

        Thread.sleep(100);
    }


    private Flux<TestMessage> processTestMessageFlux(TestMessage testMessage1) {
        Flux<TestMessage> testMessageProcessingFlux = Flux.just(testMessage1)
                .publishOn(Schedulers.parallel())
                .doOnNext(testMessage2 -> doSomeHeavyProcessing(testMessage2))
                .publishOn(Schedulers.single())
                .doOnNext(testMessage2 -> doSomeHeavyProcessing(testMessage2))
                .publishOn(Schedulers.single());
        return testMessageProcessingFlux;
    }

    public List<SequentialInput<TestMessage, TestMessage>> createSequentialMessage(int entityCount,
                                                                                   int messageCount,
                                                                                   Function<TestMessage, Flux<TestMessage>> processingFluxCreator) {

        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputs = new ArrayList<>();

        for (int messageNo = 1; messageNo <= messageCount; messageNo++) {
            for (int entityId = 1; entityId <= entityCount; entityId++) {
                TestMessage testMessage = new TestMessage(entityId, messageNo);
                SequentialInput<TestMessage, TestMessage> sequentialInput = createSequentialInput(processingFluxCreator, entityId, testMessage);
                sequentialInputs.add(sequentialInput);
            }
        }
        return sequentialInputs;
    }

    private SequentialInput<TestMessage, TestMessage> createSequentialInput(Function<TestMessage, Flux<TestMessage>> processingFluxCreator,
                                                                            int entityId,
                                                                            TestMessage testMessage) {

        LoggingInfo loggingInfo = new LoggingInfo("entity id " + entityId);

        SequentialInput<TestMessage, TestMessage> sequentialInput = new SequentialInput<>(
                testMessage,
                new TestQueueResolver(),
                new SequentialFluxSubscriber<>(
                        testMessage,
                        processingFluxCreator,
                        m -> {
                        },
                        e -> {
                            throw new RuntimeException(e);
                        },
                        loggingInfo,
                        Schedulers.parallel()),
                loggingInfo
        );
        return sequentialInput;
    }

    private void doSomeHeavyProcessing(TestMessage testMessage) {
        try {
            Thread.sleep(HEAVY_PROCESSING_MILLIS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
