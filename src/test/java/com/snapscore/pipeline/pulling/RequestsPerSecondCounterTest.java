package com.snapscore.pipeline.pulling;

import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Random;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.Assert.*;

public class RequestsPerSecondCounterTest {

    private final LocalDateTime time = LocalDateTime.of(2020, 1, 1, 12, 0, 1);
    private final LocalDateTime prevRequestTime = time.minusSeconds(1);


    @Test
    public void isRequestWithinLimitBasicTest() {

        int requestsPerSecondLimit = 10;
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(requestsPerSecondLimit, prevRequestTime);

        for (int count = 1; count <= requestsPerSecondLimit + 1; count++) {
            if (count <= requestsPerSecondLimit) {
                assertTrue("isRequestWithinLimit for count " + count + " must be true!", requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(time));
            } else {
                // last request should not be within limit
                assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(time));
            }
        }

    }

    @Test
    public void isRequestWithinLimitWhenNewRequestTimeAfterOneWholeSecondHasPassed() {

        int requestsPerSecondLimit = 10;
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(requestsPerSecondLimit, prevRequestTime);

        for (int second = 0; second < 2; second++) {
            LocalDateTime rqTime = time.plusSeconds(second);
            for (int count = 1; count <= requestsPerSecondLimit + 1; count++) {
                if (count <= requestsPerSecondLimit) {
                    assertTrue("isRequestWithinLimit for count " + count + " must be true!", requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(rqTime));
                } else {
                    // last request should not be within limit
                    assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(rqTime));
                }
            }
        }

    }

    @Test
    public void isRequestWithinLimitWhenNewRequestTimeAfterNotWholeSecondHasPassed() {

        int requestsPerSecondLimit = 2;
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(requestsPerSecondLimit, prevRequestTime);

        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(time));
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(time.plus(100, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(time.plus(200, MILLIS)));

        LocalDateTime timeAfterOneSecond = time.plusSeconds(1);
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneSecond));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneSecond));
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneSecond.plus(100, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneSecond.plus(200, MILLIS)));

        LocalDateTime timeAfterTwoSeconds = time.plusSeconds(2);
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterTwoSeconds.plus(90, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterTwoSeconds.plus(95, MILLIS)));
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterTwoSeconds.plus(100, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterTwoSeconds.plus(100, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterTwoSeconds.plus(300, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterTwoSeconds.plus(900, MILLIS)));

        LocalDateTime timeAfterOneMinuteAndSecond = time.plusMinutes(1).plusSeconds(1);
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneMinuteAndSecond));
        assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneMinuteAndSecond.plus(90, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneMinuteAndSecond.plus(100, MILLIS)));
        assertFalse(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(timeAfterOneMinuteAndSecond.plus(200, MILLIS)));
    }

    @Test
    public void testForAllSeconds() {

        int requestsPerSecondLimit = 10;
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(requestsPerSecondLimit, prevRequestTime);

        for (int second = 0; second < 60; second++) {
            LocalDateTime rqTime = time.plusSeconds(second);
            assertTrue(requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(rqTime));
        }

    }

    // just for exploring the log outputs
    @Ignore
    @Test
    public void testPassageOfTime() throws InterruptedException {

        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(1);


        for (int second = 0; second < 120; second++) {

            for (int attemptNo = 0; attemptNo < 10; attemptNo++) {
                LocalDateTime now = LocalDateTime.now();
                boolean withinLimit = requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(now);
                System.out.println( "now = " + now + "; second = " + second + "; withinLimit = " + withinLimit);
                Thread.sleep(100);
            }

        }

    }

    // just for exploring the log outputs
    @Ignore
    @Test
    public void testPassageOfTime2() {

        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(10, prevRequestTime);

        LocalDateTime prevTime = time;
        Random random = new Random(1000);


        for (int second = 0; second < 1000; second++) {

            for (int attemptNo = 0; attemptNo < 100; attemptNo++) {
                int nextRqMillis = random.nextInt(100);
                LocalDateTime currTime = prevTime.plus(nextRqMillis, MILLIS);
                prevTime = currTime;
                boolean withinLimit = requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(currTime);
                System.out.println(currTime + " = " + withinLimit);
            }

        }

    }
}
