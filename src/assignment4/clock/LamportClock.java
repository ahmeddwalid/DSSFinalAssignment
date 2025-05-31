package assignment4.clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements a Lamport logical clock.
 * This clock is used to assign timestamps to events to determine their causal order.
 * The implementation is thread-safe using AtomicLong.
 */
public class LamportClock {
    private final AtomicLong time; // Thread-safe long value for the logical time

    /**
     * Initializes the Lamport clock with a time of 0.
     */
    public LamportClock() {
        this.time = new AtomicLong(0);
    }

    /**
     * Increments the logical clock time by 1.
     * This is typically called before a local event occurs (e.g., sending a message, creating a transaction).
     * @return The new logical time after incrementing.
     */
    public long increment() {
        return time.incrementAndGet();
    }

    /**
     * Updates the local Lamport clock time based on a received timestamp.
     * This is called when a node receives a message carrying a Lamport timestamp.
     * The local clock is set to max(local_time, received_time) + 1.
     * @param receivedTime The Lamport timestamp received from another node.
     * @return The new logical time after the update.
     */
    public long update(long receivedTime) {
        long currentTime;
        long newTime;
        do {
            currentTime = time.get(); // Get current local time
            newTime = Math.max(currentTime, receivedTime) + 1; // Calculate new time based on the rule
        } while (!time.compareAndSet(currentTime, newTime)); // Atomically update if current time hasn't changed
        return newTime;
    }

    /**
     * Gets the current logical time of the clock without incrementing it.
     * @return The current logical time.
     */
    public long getTime() {
        return time.get();
    }

    /**
     * Sets the clock to a specific time.
     * This method should be used with caution, primarily for testing or initialization scenarios.
     * @param newTime The new time value to set the clock to.
     */
    public void setTime(long newTime) {
        time.set(newTime);
    }

    @Override
    public String toString() {
        return "LamportClock{time=" + time.get() + "}";
    }
}
