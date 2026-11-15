package com.mj.raft.testutil;

import java.time.Duration;
import java.util.function.BooleanSupplier;

public final class Await {

    private static final long POLL_INTERVAL_MS = 50;

    private Await() {
    }

    /**
     * Polls condition until it is true, failing if timeout elapses first.
     * Polling is used because the server does not signal state changes.
     */
    public static void until(Duration timeout, String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout + " waiting for: " + what);
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
    }

    /**
     * Polls condition for the whole duration, failing as soon as it is false.
     * For checking that something bad never happens, e.g. a minority electing a leader.
     */
    public static void holds(Duration duration, String what, BooleanSupplier condition) throws InterruptedException {
        long start = System.nanoTime();
        long end = start + duration.toNanos();
        while (System.nanoTime() < end) {
            if (!condition.getAsBoolean()) {
                throw new AssertionError("After " + Duration.ofNanos(System.nanoTime() - start).toMillis()
                        + "ms no longer true: " + what);
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
    }
}
