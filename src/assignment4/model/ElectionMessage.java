package assignment4.model;

import java.io.Serializable;

/**
 * Represents a message used in the Bully Algorithm election process and for transaction communication.
 * This class is Serializable to allow it to be sent over a network (if needed).
 */
public class ElectionMessage implements Serializable {
    private static final long serialVersionUID = 1L; // Recommended for Serializable classes

    private final MessageType type;                 // The type of the message (e.g., ELECTION, OK, TRANSACTION)
    private final int senderNodeId;               // The ID of the node sending this message
    private final int targetNodeId;               // The ID of the node intended to receive this message (can be a broadcast ID)
    private final long messageTimestamp;          // Lamport timestamp of the sender when the message was created or system time
    private final TimestampedTransaction transaction; // Payload for TRANSACTION messages, null otherwise

    /**
     * Constructor for messages that do not carry a transaction payload (e.g., ELECTION, OK, COORDINATOR, HEARTBEAT).
     * @param type The type of the message.
     * @param senderNodeId The ID of the sender node.
     * @param targetNodeId The ID of the target node.
     * @param messageTimestamp The Lamport timestamp of the sender or system time.
     */
    public ElectionMessage(MessageType type, int senderNodeId, int targetNodeId, long messageTimestamp) {
        this.type = type;
        this.senderNodeId = senderNodeId;
        this.targetNodeId = targetNodeId;
        this.messageTimestamp = messageTimestamp;
        this.transaction = null; // No transaction payload for these types
    }

    /**
     * Constructor for messages that carry a transaction payload (i.e., TRANSACTION messages).
     * @param type The type of the message (should be MessageType.TRANSACTION).
     * @param senderNodeId The ID of the sender node.
     * @param targetNodeId The ID of the target node (usually the leader).
     * @param messageTimestamp The Lamport timestamp of the sender or system time.
     * @param transaction The transaction being sent.
     */
    public ElectionMessage(MessageType type, int senderNodeId, int targetNodeId, long messageTimestamp, TimestampedTransaction transaction) {
        if (type != MessageType.TRANSACTION && transaction != null) {
            // Or log a warning, this constructor is primarily for TRANSACTION type
        }
        this.type = type;
        this.senderNodeId = senderNodeId;
        this.targetNodeId = targetNodeId;
        this.messageTimestamp = messageTimestamp;
        this.transaction = transaction;
    }

    // Getters
    public MessageType getType() { return type; }
    public int getSenderNodeId() { return senderNodeId; }
    public int getTargetNodeId() { return targetNodeId; }
    public long getMessageTimestamp() { return messageTimestamp; } // Renamed from getTimestamp to avoid conflict with potential transaction timestamp
    public TimestampedTransaction getTransaction() { return transaction; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Msg{%s, from:%d, to:%d, ts:%d",
                type, senderNodeId, targetNodeId, messageTimestamp));
        if (transaction != null) {
            sb.append(", payload:").append(transaction.toString());
        }
        sb.append("}");
        return sb.toString();
    }
}
