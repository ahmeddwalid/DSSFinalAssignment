package assignment4.model;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a transaction with Lamport timestamp for ordering.
 * This class is Serializable to allow it to be sent over a network (if needed)
 * and Comparable to allow sorting in a priority queue.
 */
public class TimestampedTransaction implements Serializable, Comparable<TimestampedTransaction> {
    private static final long serialVersionUID = 1L; // Recommended for Serializable classes
    private static final AtomicLong transactionCounter = new AtomicLong(0); // Global counter for unique transaction IDs

    private final long transactionId;         // Unique ID for the transaction
    private final String operation;           // Description of the transaction operation
    private final int sourceNodeId;           // ID of the node that originated this transaction
    private final long lamportTimestamp;      // Lamport timestamp assigned at creation
    private final long creationTime;          // System time of creation for logging/debugging

    /**
     * Constructor for TimestampedTransaction.
     * @param operation The operation this transaction represents (e.g., "Transfer $100 from A to B").
     * @param sourceNodeId The ID of the node creating this transaction.
     * @param lamportTimestamp The Lamport timestamp from the source node at the time of creation.
     */
    public TimestampedTransaction(String operation, int sourceNodeId, long lamportTimestamp) {
        this.transactionId = transactionCounter.incrementAndGet(); // Assign a unique ID
        this.operation = operation;
        this.sourceNodeId = sourceNodeId;
        this.lamportTimestamp = lamportTimestamp;
        this.creationTime = System.currentTimeMillis(); // Record creation time
    }

    /**
     * Compares this transaction with another for ordering.
     * Primary ordering is by Lamport timestamp.
     * Secondary ordering (tie-breaking) is by source node ID (lower ID preferred).
     * Tertiary ordering (further tie-breaking) is by transaction ID.
     * @param other The other TimestampedTransaction to compare against.
     * @return a negative integer, zero, or a positive integer as this transaction
     * is less than, equal to, or greater than the specified transaction.
     */
    @Override
    public int compareTo(TimestampedTransaction other) {
        // Primary order: Lamport timestamp (lower timestamp first)
        int timestampComparison = Long.compare(this.lamportTimestamp, other.lamportTimestamp);
        if (timestampComparison != 0) {
            return timestampComparison;
        }

        // Tie-breaker 1: Source Node ID (lower ID first)
        int nodeComparison = Integer.compare(this.sourceNodeId, other.sourceNodeId);
        if (nodeComparison != 0) {
            return nodeComparison;
        }

        // Tie-breaker 2: Transaction ID (lower ID first, ensures total order for transactions from same node at same Lamport time)
        return Long.compare(this.transactionId, other.transactionId);
    }

    // Getters
    public long getTransactionId() { return transactionId; }
    public String getOperation() { return operation; }
    public int getSourceNodeId() { return sourceNodeId; }
    public long getLamportTimestamp() { return lamportTimestamp; }
    public long getCreationTime() { return creationTime; }

    @Override
    public String toString() {
        return String.format("Tx[%d](ts:%d,node:%d) - '%s'",
                transactionId, lamportTimestamp, sourceNodeId, operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof TimestampedTransaction)) return false;
        TimestampedTransaction other = (TimestampedTransaction) obj;
        // Transactions are considered equal if they have the same ID.
        return transactionId == other.transactionId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(transactionId);
    }
}
