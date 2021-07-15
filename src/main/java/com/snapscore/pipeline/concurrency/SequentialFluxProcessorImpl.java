package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class SequentialFluxProcessorImpl implements SequentialFluxProcessor {

    private static final Logger logger = Logger.setup(SequentialFluxProcessorImpl.class);

    private static final int INPUT_QUEUES_COUNT_DEFAULT = 10000;
    public static final String UNPROCESSED_TOTAL_LOG_ANALYTICS_ID = "unprocessed_total";

    private final String name;
    private final int inputQueueCount;

    // as we need to ensure that access to the message queues is atomic we need to lock on this object
    private final Object queueLock = new Object();
    // guarded by "queueLock"
    private final Map<Integer, Queue<EnqueuedInput>> inputQueues = new ConcurrentHashMap<>();
    // guarded by "queueLock"
    private final AtomicLong totalEnqueuedInputs = new AtomicLong(0);
    private volatile CompletableFuture<Void> future;

    /**
     * @param inputQueueCount should be a big enough number for the passed messages to get spread out evenly
     * @param name if multiple instances are created
     */
    public SequentialFluxProcessorImpl(int inputQueueCount, String name) {
        this.name = name;
        this.inputQueueCount = inputQueueCount;
        for (int queueIdx = 0; queueIdx < this.inputQueueCount; queueIdx++) {
            inputQueues.put(queueIdx, new LinkedList<>());
        }
    }

    public SequentialFluxProcessorImpl(String name) {
        this(INPUT_QUEUES_COUNT_DEFAULT, name);
    }

    @Override
    public <I, R> void processSequentiallyAsync(SequentialInput<I, R> sequentialInput) {
        int queueIdx = sequentialInput.queueResolver.getQueueIdxFor(sequentialInput.input, inputQueueCount);
        EnqueuedInput enqueuedInput = new EnqueuedInput(queueIdx, sequentialInput.input, sequentialInput.sequentialFluxSubscriber, sequentialInput.loggingInfo);
        enqueueAndProcess(enqueuedInput);
    }

    @Override
    public void awaitProcessingCompletion(Duration timeout) throws Exception {
        if (totalEnqueuedInputs.get() > 0L && this.future != null) {
            this.future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public long getTotalUnprocessedInputs() {
        return totalEnqueuedInputs.get();
    }

    private void enqueueAndProcess(EnqueuedInput enqueuedInput) {
        boolean canProcessImmediately;
        Logger loggerDecorated = enqueuedInput.loggingInfo.decorate(logger);
        int queueIdx = enqueuedInput.queueIdx;
        int queueSize;
        synchronized (queueLock) {
            Queue<EnqueuedInput> queue = inputQueues.get(queueIdx);
            if (queue == null) {
                loggerDecorated.error("{}: Failed to find queue for queue no. {}", this.name, queueIdx);
                queue = new LinkedList<>();
                inputQueues.put(queueIdx, queue);
            }
            canProcessImmediately = queue.isEmpty();
            queue.add(enqueuedInput);
            queueSize = queue.size();
            if (totalEnqueuedInputs.get() == 0L) {
                this.future = new CompletableFuture<>();
            }
            totalEnqueuedInputs.incrementAndGet();
        }
        if (enqueuedInput.loggingInfo.logActivity) {
            loggerDecorated.decorateSetup(props -> props.analyticsId(UNPROCESSED_TOTAL_LOG_ANALYTICS_ID).exec(String.valueOf(totalEnqueuedInputs.get())))
                    .info("{}: Input queue no. {} size = {}; Enqueued inputs total = {}. Just enqueued input {}", this.name, queueIdx, queueSize, totalEnqueuedInputs.get(), enqueuedInput.loggingInfo.inputDescription);
            loggerDecorated.info("canProcessImmediately = {} for input {}", canProcessImmediately, enqueuedInput.loggingInfo.inputDescription);
        }
        if (canProcessImmediately) {
            // if no previous item is being processed then we can send this one immediately
            processNext(enqueuedInput);
        }
    }

    private void processNext(EnqueuedInput enqueuedInput) {
        if (enqueuedInput.loggingInfo.logActivity) {
            Logger loggerDecorated = enqueuedInput.loggingInfo.decorate(logger);
            loggerDecorated.info("{}: Going to process next input: {}", this.name, enqueuedInput.loggingInfo.inputDescription);
        }
        logIfWaitingForTooLong(enqueuedInput);
        enqueuedInput.sequentialFluxSubscriber.subscribe( // Subscribing with these hoods is EXTREMELY important to ensure that the next message is taken from the queue and processed
                () -> dequeueCurrentAndProcessNext(enqueuedInput),
                () -> dequeueCurrentAndProcessNext(enqueuedInput),
                enqueuedInput.enqueuedTs
        );
    }

    private void dequeueCurrentAndProcessNext(EnqueuedInput currInput) {
        Logger loggerDecorated = currInput.loggingInfo.decorate(logger);
        try {
            if (currInput.loggingInfo.logActivity) {
                loggerDecorated.info("{}: Entered dequeueCurrentAndProcessNext after finished processing input: {}", this.name, currInput.loggingInfo.inputDescription);
            }
            EnqueuedInput nextInput;
            int newQueueSize;
            int queueIdx = currInput.queueIdx;
            synchronized (queueLock) {
                Queue<EnqueuedInput> queue = inputQueues.get(queueIdx);
                queue.poll(); // dequeue the previously processed item
                totalEnqueuedInputs.decrementAndGet();
                if (totalEnqueuedInputs.get() == 0L) {
                    this.future.complete(null);
                }
                newQueueSize = queue.size();
                nextInput = queue.peek();
            }
            if (currInput.loggingInfo.logActivity) {
                loggerDecorated.decorateSetup(props -> props.analyticsId(UNPROCESSED_TOTAL_LOG_ANALYTICS_ID).exec(String.valueOf(totalEnqueuedInputs.get())))
                        .info("{}: Input queue no. {} size = {}; Enqueued inputs total = {}. ... after polling last processed input: {}", this.name, queueIdx, newQueueSize, totalEnqueuedInputs.get(), currInput.loggingInfo.inputDescription);
            }
            if (nextInput != null) {
                processNext(nextInput);
            }
        } catch (Exception e) {
            loggerDecorated.error("{}: Error inside dequeueCurrentAndProcessNext! {}", this.name, currInput.loggingInfo.inputDescription);
        }
    }

    private void logIfWaitingForTooLong(EnqueuedInput input) {
        long waitingMillis = System.currentTimeMillis() - input.enqueuedTs;
        if (waitingMillis > 2_000) {
            logger.decorateSetup(mdc -> mdc.analyticsId("enqueued_input_for_too_long")).warn("{}: EnqueuedInput waiting too long for processing: {} ms; Enqueued inputs total = {}; input: {}", this.name, waitingMillis, totalEnqueuedInputs.get(), input.loggingInfo.inputDescription);
        }
    }


    private static class EnqueuedInput {

        private final int queueIdx;
        private final Object inputData;
        private final SequentialFluxSubscriber<?, ?> sequentialFluxSubscriber;
        private final LoggingInfo loggingInfo;
        private final long enqueuedTs;

        public EnqueuedInput(int queueIdx,
                             Object inputData,
                             SequentialFluxSubscriber<?, ?> sequentialFluxSubscriber,
                             LoggingInfo loggingInfo) {
            this.queueIdx = queueIdx;
            this.inputData = inputData;
            this.sequentialFluxSubscriber = sequentialFluxSubscriber;
            this.loggingInfo = loggingInfo;
            this.enqueuedTs = System.currentTimeMillis();
        }
    }


}
