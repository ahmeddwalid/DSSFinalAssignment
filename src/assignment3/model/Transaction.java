package assignment3.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents a banking transaction with Lamport timestamp
 * Immutable class for thread safety
 */
public class Transaction {
    private final String id;
    private final String type; // Should be Transaction.Type
    private final double amount;
    private final String account;
    private final int lamportTimestamp;
    private final int sourceNodeId;
    private final long creationTime;

    /**
     * Transaction types enumeration
     */
    public enum Type {
        DEPOSIT, WITHDRAWAL, TRANSFER
    }

    /**
     * Constructor for Transaction
     * @param type transaction type (DEPOSIT, WITHDRAWAL, TRANSFER as String)
     * @param amount transaction amount
     * @param account target account
     * @param lamportTimestamp Lamport timestamp for ordering
     * @param sourceNodeId ID of the node that created this transaction
     */
    public Transaction(String type, double amount, String account, int lamportTimestamp, int sourceNodeId) {
        this.id = UUID.randomUUID().toString();
        this.type = type; // Consider using Transaction.Type enum here directly
        this.amount = amount;
        this.account = account;
        this.lamportTimestamp = lamportTimestamp;
        this.sourceNodeId = sourceNodeId;
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Constructor for Transaction using Enum Type
     * @param type transaction type (Transaction.Type enum)
     * @param amount transaction amount
     * @param account target account
     * @param lamportTimestamp Lamport timestamp for ordering
     * @param sourceNodeId ID of the node that created this transaction
     */
    public Transaction(Type type, double amount, String account, int lamportTimestamp, int sourceNodeId) {
        this.id = UUID.randomUUID().toString();
        this.type = type.name(); // Store as String or keep as Type
        this.amount = amount;
        this.account = account;
        this.lamportTimestamp = lamportTimestamp;
        this.sourceNodeId = sourceNodeId;
        this.creationTime = System.currentTimeMillis();
    }

    // Getters
    public String getId() { return id; }
    public String getType() { return type; } // Consider returning Transaction.Type
    public double getAmount() { return amount; }
    public String getAccount() { return account; }
    public int getLamportTimestamp() { return lamportTimestamp; }
    public int getSourceNodeId() { return sourceNodeId; }
    public long getCreationTime() { return creationTime; }

    /**
     * Check if this transaction is considered stale based on current time
     * @param currentLamportTime current Lamport timestamp
     * @return true if transaction is stale
     */
    public boolean isStale(int currentLamportTime) {
        // Configurable staleness threshold, e.g. if a transaction's timestamp is much older
        // than the current node's Lamport time, it might be considered stale.
        // The definition of "stale" can vary. Here, it's a simple difference.
        return (currentLamportTime - this.lamportTimestamp) > 10; // Example threshold
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Transaction that = (Transaction) obj;
        return Objects.equals(id, that.id); // Transactions are unique by ID
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("Transaction{id='%s', type='%s', amount=%.2f, account='%s', lamport=%d, source=%d}",
                id.substring(0, 8), type, amount, account, lamportTimestamp, sourceNodeId);
    }
}
