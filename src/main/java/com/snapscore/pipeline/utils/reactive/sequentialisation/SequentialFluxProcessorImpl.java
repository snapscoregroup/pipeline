package com.snapscore.pipeline.utils.reactive.sequentialisation;

import com.snapscore.pipeline.logging.Logger;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class SequentialFluxProcessorImpl implements SequentialFluxProcessor {

    private static final Logger logger = Logger.setup(SequentialFluxProcessorImpl.class);

    private static final int INPUT_QUEUES_COUNT_DEFAULT = 100000;
    private final int inputQueueCount;

    // as we need to ensure that access to the message queues is atomic we need to lock on this object
    private final Object queueLock = new Object();
    // guarded by "queueLock"
    private final Map<Integer, Queue<EnqueuedInput>> inputQueues = new ConcurrentHashMap<>();
    // guarded by "queueLock"
    private final AtomicLong totalEnqueuedInputs = new AtomicLong(0);

    /**
     * @param inputQueueCount should be a big enough number for the passed messages to get spread out evenly
     */
    public SequentialFluxProcessorImpl(int inputQueueCount) {
        this.inputQueueCount = inputQueueCount;
        for (int queueIdx = 0; queueIdx < this.inputQueueCount; queueIdx++) {
            inputQueues.put(queueIdx, new LinkedList<>());
        }
    }

    public SequentialFluxProcessorImpl() {
        this(INPUT_QUEUES_COUNT_DEFAULT);
    }

    @Override
    public <I, R> void processSequentially(SequentialInput<I, R> sequentialInput) {
        int queueIdx = sequentialInput.queueResolver.getQueueIdxFor(sequentialInput.input, inputQueueCount);
        EnqueuedInput enqueuedInput = new EnqueuedInput(queueIdx, sequentialInput.input, sequentialInput.sequentialFluxSubscriber, sequentialInput.loggingInfo);
        enqueueAndProcess(enqueuedInput);
    }

    private void enqueueAndProcess(EnqueuedInput enqueuedInput) {
        boolean canProcessImmediately;
        Logger loggerDecorated = enqueuedInput.loggingInfo.decorate(logger);
        synchronized (queueLock) {
            Queue<EnqueuedInput> queue = inputQueues.get(enqueuedInput.queueIdx);
            if (queue == null) {
                loggerDecorated.error("Failed to find queue for queueIdx {}", enqueuedInput.queueIdx);
                queue = new LinkedList<>();
                inputQueues.put(enqueuedInput.queueIdx, queue);
            }
            canProcessImmediately = queue.isEmpty();
            queue.add(enqueuedInput);
            totalEnqueuedInputs.incrementAndGet();
            loggerDecorated.info("EnqueuedInput queue size = {}. Enqueued inputs total = {}. Last enqueued input {}", queue.size(), totalEnqueuedInputs.get(), enqueuedInput.loggingInfo.getMessage());
        }
        loggerDecorated.info("canProcessImmediately = {} for input {}", canProcessImmediately, enqueuedInput.loggingInfo.getMessage());
        if (canProcessImmediately) {
            // if no previous item is being processed then we can send this one immediately
            processNext(enqueuedInput);
        }
    }

    private void processNext(EnqueuedInput enqueuedInput) {
        Logger loggerDecorated = enqueuedInput.loggingInfo.decorate(logger);
        loggerDecorated.info("Entered processNext {}", enqueuedInput.loggingInfo.getMessage());
        logIfWaitingForTooLong(enqueuedInput);
        enqueuedInput.sequentialFluxSubscriber.subscribe( // Subscribing with these hoods is EXTREMELY important to ensure that the next message is taken from the queue and processed
                () -> dequeueCurrentAndProcessNext(enqueuedInput),
                () -> dequeueCurrentAndProcessNext(enqueuedInput)
        );
    }

    private void dequeueCurrentAndProcessNext(EnqueuedInput currElement) {
        Logger loggerDecorated = currElement.loggingInfo.decorate(logger);
        try {
            loggerDecorated.info("Entered dequeueCurrentAndProcessNext {}", currElement.loggingInfo.getMessage());
            EnqueuedInput nextInput;
            int newQueueSize;
            synchronized (queueLock) {
                Queue<EnqueuedInput> queue = inputQueues.get(currElement.queueIdx);
                queue.poll(); // dequeue the previously processed item
                totalEnqueuedInputs.decrementAndGet();
                newQueueSize = queue.size();
                nextInput = queue.peek();
            }
            loggerDecorated.info("Enqueued inputs total = {} and queue size = {} after polling last processed message: {}", totalEnqueuedInputs.get(), newQueueSize, currElement.loggingInfo.getMessage());
            if (nextInput != null) {
                processNext(nextInput);
            }
        } catch (Exception e) {
            loggerDecorated.error("Error inside dequeueCurrentAndProcessNext! {}", currElement.loggingInfo.getMessage());
        }
    }

    private void logIfWaitingForTooLong(EnqueuedInput input) {
        long waitingMillis = System.currentTimeMillis() - input.createdTs;
        if (waitingMillis > 2_000) {
            logger.decorateSetup(mdc -> mdc.exec("enqueued_input_for_too_long")).warn("EnqueuedInput waiting too long for processing: {} ms; Enqueued inputs total = {}; input: {}", waitingMillis, totalEnqueuedInputs.get(), input.loggingInfo.getMessage());
        }
    }


    private static class EnqueuedInput {

        private final int queueIdx;
        private final Object inputData;
        private final SequentialFluxSubscriber<?, ?> sequentialFluxSubscriber;
        private final LoggingInfo loggingInfo;
        private final long createdTs;

        public EnqueuedInput(int queueIdx,
                             Object inputData,
                             SequentialFluxSubscriber<?, ?> sequentialFluxSubscriber,
                             LoggingInfo loggingInfo) {
            this.queueIdx = queueIdx;
            this.inputData = inputData;
            this.sequentialFluxSubscriber = sequentialFluxSubscriber;
            this.loggingInfo = loggingInfo;
            this.createdTs = System.currentTimeMillis();
        }
    }


}
