package com.snapscore.pipeline.pulling;

import org.junit.Test;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.junit.Assert.assertEquals;

public class PriorityBlockingQueueOrderingTest {

    @Test
    public void testOrderOfInsertedElementsIsStable() {

        Queue<Item> priorityQueue = new PriorityBlockingQueue<>(100, Item.QUEUE_COMPARATOR);

        priorityQueue.add(new Item(FeedPriorityEnum.HIGH, 1));
        priorityQueue.add(new Item(FeedPriorityEnum.MEDIUM, 2));
        priorityQueue.add(new Item(FeedPriorityEnum.HIGH, 3));
        priorityQueue.add(new Item(FeedPriorityEnum.MEDIUM, 4));
        priorityQueue.add(new Item(FeedPriorityEnum.HIGH, 5));

        assertEquals(1, priorityQueue.poll().value);
        assertEquals(3, priorityQueue.poll().value);
        assertEquals(5, priorityQueue.poll().value);
        assertEquals(2, priorityQueue.poll().value);
        assertEquals(4, priorityQueue.poll().value);

    }


    private static class Item {

        public static final Comparator<Item> QUEUE_COMPARATOR = Comparator.comparingInt(Item::getSchedulingOrder).thenComparingLong(Item::getCreatedTimeNanos);

        private final FeedPriorityEnum priority;
        private final int value;
        private final long createdTimeNanos;

        public Item(FeedPriorityEnum priority, int value) {
            this.priority = priority;
            this.value = value;
            this.createdTimeNanos = System.nanoTime();
        }

        private int getSchedulingOrder() {
            return priority.getSchedulingOrder();
        }

        public long getCreatedTimeNanos() {
            return createdTimeNanos;
        }

    }


}
