package com.snapscore.pipeline.utils.reactive.sequentialisation;

import junit.framework.TestCase;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.snapscore.pipeline.utils.reactive.sequentialisation.TestSupport.*;

public class SequentialFluxProcessorTest extends TestCase {

    public static final int HEAVY_PROCESSING_MILLIS = 0;

    @Test
    public void testThatMessagesOfSingleEntityAreProcessedSequentiallyAndInCorrectOrder() throws Exception {

        // given
        final SequentialFluxProcessor sequentialProcessor = new SequentialFluxProcessorImpl("test-flux-processor");
        final Map<Integer, TestMessage> prevProcessedTestMessageMap = new ConcurrentHashMap<>();
        final int entityCount = 1;
        int messageCount = 10000;

        // then
        final Consumer<TestMessage> assertion = m -> {
            boolean isCorrectNextMessage = prevProcessedTestMessageMap.get(m.entityId).messageNo + 1 == m.messageNo;
            prevProcessedTestMessageMap.put(m.entityId, m);
            assertTrue("Error in processed message order!", isCorrectNextMessage);
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputData = createSequentialMessage(prevProcessedTestMessageMap, entityCount, messageCount, assertion, this::processTestMessageFlux);

        // when
        sequentialInputData.forEach(sequentialProcessor::processSequentiallyAsync);

        sequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(2000));
    }

    @Test
    public void testThatMessagesOfMultipleEntitiesAreProcessedSequentiallyAndInCorrectOrder() throws Exception {

        // given
        final SequentialFluxProcessor sequentialProcessor = new SequentialFluxProcessorImpl("test-flux-processor");
        final Map<Integer, TestMessage> prevProcessedTestMessageMap = new ConcurrentHashMap<>();
        final int entityCount = 10;
        int messageCount = 1000;

        // then
        final Consumer<TestMessage> assertion = m -> {
            boolean isCorrectNextMessage = prevProcessedTestMessageMap.get(m.entityId).messageNo + 1 == m.messageNo;
            prevProcessedTestMessageMap.put(m.entityId, m);
            assertTrue("Error in processed message order!", isCorrectNextMessage);
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputData = createSequentialMessage(prevProcessedTestMessageMap, entityCount, messageCount, assertion, this::processTestMessageFlux);

        // when
        sequentialInputData.forEach(sequentialProcessor::processSequentiallyAsync);

        sequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(2000));
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

    public List<SequentialInput<TestMessage, TestMessage>> createSequentialMessage(Map<Integer, TestMessage> prevProcessedTestMessageMap,
                                                                                   int entityCount,
                                                                                   int messageCount,
                                                                                   Consumer<TestMessage> assertion,
                                                                                   Function<TestMessage, Flux<TestMessage>> processingFluxCreator) {
        IntStream.range(1, entityCount + 1).forEach(entityId -> {
            prevProcessedTestMessageMap.put(entityId, new TestMessage(entityId, 0));  // prepopulate with base message to ovoid NPE later ..
        });
        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputs = new ArrayList<>();

        for (int messageNo = 1; messageNo <= messageCount; messageNo++) {
            for (int entityId = 1; entityId <= entityCount; entityId++) {
                TestMessage testMessage = new TestMessage(entityId, messageNo);
                LoggingInfo loggingInfo = new LoggingInfo(true, "entity id " + entityId);
                SequentialInput<TestMessage, TestMessage> sequentialInput = new SequentialInput<>(
                        testMessage,
                        new TestQueueResolver(),
                        new SequentialFluxSubscriber<>(
                                testMessage,
                                processingFluxCreator,
                                m ->  assertion.accept(m),
                                e -> {
                                    throw new RuntimeException(e);
                                },
                                loggingInfo,
                                Schedulers.parallel()),
                        loggingInfo
                );
                sequentialInputs.add(sequentialInput);
            }
        }
        return sequentialInputs;
    }

    private void doSomeHeavyProcessing(TestMessage testMessage) {
        try {
            Thread.sleep(HEAVY_PROCESSING_MILLIS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
