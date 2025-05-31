package assignment4.model;

/**
 * Defines the types of messages used in the Enhanced Bully Algorithm and transaction processing.
 */
public enum MessageType {
    /**
     * Message sent by a node to initiate an election.
     */
    ELECTION,

    /**
     * Message sent by a higher-numbered node in response to an ELECTION message,
     * indicating it is alive and will take over the election.
     */
    OK,

    /**
     * Message sent by a node that has won the election to announce itself as the new leader (coordinator).
     */
    COORDINATOR,

    /**
     * Message sent periodically by the leader to other nodes to indicate it is still alive.
     */
    HEARTBEAT,

    /**
     * Message sent by a node to the leader, containing a transaction to be processed.
     */
    TRANSACTION,

    /**
     * Message sent by a node in response to a HEARTBEAT from the leader, confirming it has received the heartbeat.
     * (Also implies the sender is alive).
     */
    ALIVE,

    /**
     * Message to request the current leader ID.
     */
    REQUEST_LEADER,

    /**
     * Message containing the current leader ID.
     */
    LEADER_INFO
}
