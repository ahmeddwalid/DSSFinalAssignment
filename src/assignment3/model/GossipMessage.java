package assignment3.model;

/**
 * Wrapper for gossip protocol messages
 * Contains the transaction being gossiped and some routing/metadata.
 * This class is immutable.
 */
public class GossipMessage {
    private final Transaction transaction; // The actual transaction data
    private final int senderNodeId;      // ID of the node that sent this specific gossip message instance
    private final int hopCount;          // How many nodes this message has passed through
    private final long timestamp;        // Creation timestamp of this GossipMessage object

    /**
     * Constructor for GossipMessage.
     * @param transaction The transaction to be gossiped.
     * @param senderNodeId The ID of the node sending this message.
     * @param hopCount The number of hops this message has already made.
     */
    public GossipMessage(Transaction transaction, int senderNodeId, int hopCount) {
        if (transaction == null) {
            throw new IllegalArgumentException("Transaction cannot be null.");
        }
        this.transaction = transaction;
        this.senderNodeId = senderNodeId;
        this.hopCount = hopCount;
        this.timestamp = System.currentTimeMillis(); // Timestamp of when this GossipMessage object was created
    }

    /**
     * Creates a new GossipMessage instance for forwarding, with an incremented hop count.
     * This is useful when a node receives a gossip message and decides to propagate it further.
     * @param newSenderNodeId The ID of the current node that is forwarding the message.
     * @return A new GossipMessage instance representing the forwarded message.
     */
    public GossipMessage forward(int newSenderNodeId) {
        return new GossipMessage(this.transaction, newSenderNodeId, this.hopCount + 1);
    }

    // Getters
    public Transaction getTransaction() { return transaction; }
    public int getSenderNodeId() { return senderNodeId; }
    public int getHopCount() { return hopCount; }
    public long getTimestamp() { return timestamp; } // Timestamp of this gossip message wrapper

    /**
     * Checks if the message should be dropped based on its hop count.
     * This helps prevent messages from circulating indefinitely in the network (TTL mechanism).
     * @return true if the message has exceeded the maximum allowed hops, false otherwise.
     */
    public boolean shouldDrop() {
        // Example: Maximum hop limit to prevent infinite circulation.
        // This value should be configured based on network size and desired propagation.
        final int MAX_HOPS = 5;
        return hopCount > MAX_HOPS;
    }

    @Override
    public String toString() {
        return String.format("GossipMessage{transaction=%s, sender=%d, hops=%d, created_at=%d}",
                transaction.toString(), senderNodeId, hopCount, timestamp);
    }
}
