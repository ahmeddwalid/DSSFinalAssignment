package assignment4.election;

import assignment4.model.ElectionMessage;
import assignment4.model.MessageType;
import assignment4.node.EnhancedNode;
import assignment4.network.MessageHandler; // Added import
import common.utils.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * Implements the Bully Algorithm for leader election among EnhancedNodes.
 */
public class BullyElection {
    private final EnhancedNode selfNode; // Reference to the node this election algorithm instance belongs to
    private final MessageHandler messageHandler; // For sending messages
    private final Logger logger = Logger.getInstance();
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false); // Ensures only one election is started by this node at a time
    private final Set<Integer> okResponsesReceived = ConcurrentHashMap.newKeySet(); // Stores IDs of nodes that sent OK

    private static final long ELECTION_RESPONSE_TIMEOUT_MS = 1500; // Timeout for waiting for OK messages
    private static final long COORDINATOR_MESSAGE_TIMEOUT_MS = 2000; // Timeout for waiting for a COORDINATOR message

    /**
     * Constructor for BullyElection.
     * @param selfNode The node instance this election logic is for.
     * @param messageHandler The message handler for sending messages.
     */
    public BullyElection(EnhancedNode selfNode, MessageHandler messageHandler) {
        this.selfNode = selfNode;
        this.messageHandler = messageHandler;
    }

    /**
     * Starts the leader election process.
     * A node initiates an election if it detects the leader has failed or upon startup.
     */
    public void startElection() {
        if (selfNode.isFailed() || !selfNode.isRunning()) {
            logger.debug(String.format("Node %d: Cannot start election, node is failed or not running.", selfNode.getNodeId()));
            return;
        }

        // Try to set electionInProgress to true. If it was already true, another election is ongoing.
        if (!electionInProgress.compareAndSet(false, true)) {
            logger.info(String.format("Node %d: Election already in progress.", selfNode.getNodeId()));
            return;
        }

        okResponsesReceived.clear(); // Clear previous responses
        logger.info(String.format("Node %d: Starting new election. Lamport time: %d", selfNode.getNodeId(), selfNode.getLamportClock().getTime()));
        selfNode.setLeader(false); // Assume not leader until election concludes

        List<EnhancedNode> higherIdNodes = selfNode.getPeers().stream()
                .filter(peer -> peer.getNodeId() > selfNode.getNodeId() && peer.isActive() && !peer.isFailed())
                .collect(Collectors.toList());

        if (higherIdNodes.isEmpty()) {
            // No active higher-ID nodes, this node becomes the leader.
            logger.info(String.format("Node %d: No active higher-ID nodes found. Declaring self as leader.", selfNode.getNodeId()));
            announceAsLeader();
            electionInProgress.set(false);
        } else {
            // Send ELECTION messages to all higher-ID nodes.
            logger.info(String.format("Node %d: Sending ELECTION messages to %d higher-ID nodes.", selfNode.getNodeId(), higherIdNodes.size()));
            for (EnhancedNode higherNode : higherIdNodes) {
                ElectionMessage electionMsg = new ElectionMessage(
                        MessageType.ELECTION,
                        selfNode.getNodeId(),
                        higherNode.getNodeId(),
                        selfNode.getLamportClock().getTime()); // Send current Lamport time
                messageHandler.sendMessageAsync(higherNode, electionMsg);
            }

            // Wait for OK responses or timeout.
            try {
                // This is a simplified wait. A more robust implementation would use a CountDownLatch
                // or check okResponsesReceived periodically.
                Thread.sleep(ELECTION_RESPONSE_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(String.format("Node %d: Election response wait interrupted.", selfNode.getNodeId()));
            }

            if (okResponsesReceived.isEmpty()) {
                // No OK responses received from higher-ID nodes, this node becomes leader.
                logger.info(String.format("Node %d: No OK responses received from higher-ID nodes. Declaring self as leader.", selfNode.getNodeId()));
                announceAsLeader();
            } else {
                // OK response(s) received. This node does not become leader.
                // It should now wait for a COORDINATOR message from the new leader.
                logger.info(String.format("Node %d: Received OK responses. Waiting for COORDINATOR message.", selfNode.getNodeId()));
                // A mechanism to timeout waiting for COORDINATOR and re-trigger election might be needed here.
            }
            electionInProgress.set(false);
        }
    }

    /**
     * Handles an incoming ELECTION message from another node.
     * @param message The ELECTION message received.
     */
    public void handleElectionMessage(ElectionMessage message) {
        if (selfNode.isFailed() || !selfNode.isRunning()) return;

        logger.info(String.format("Node %d: Received ELECTION message from Node %d.", selfNode.getNodeId(), message.getSenderNodeId()));

        // Send OK response back to the sender.
        EnhancedNode senderNode = selfNode.findNodeById(message.getSenderNodeId());
        if (senderNode != null) {
            ElectionMessage okMsg = new ElectionMessage(
                    MessageType.OK,
                    selfNode.getNodeId(),
                    senderNode.getNodeId(),
                    selfNode.getLamportClock().getTime()); // Send current Lamport time
            messageHandler.sendMessageAsync(senderNode, okMsg);
        } else {
            logger.warn(String.format("Node %d: Could not find sender Node %d to send OK message.", selfNode.getNodeId(), message.getSenderNodeId()));
        }

        // If this node has a higher ID, it starts its own election process.
        // (The check `!electionInProgress.get()` ensures it doesn't start multiple elections concurrently)
        if (!electionInProgress.get()) {
            startElection(); // Start own election if not already in one
        }
    }

    /**
     * Handles an incoming OK message from a higher-ID node.
     * @param message The OK message received.
     */
    public void handleOkMessage(ElectionMessage message) {
        if (selfNode.isFailed() || !selfNode.isRunning()) return;

        logger.info(String.format("Node %d: Received OK message from Node %d.", selfNode.getNodeId(), message.getSenderNodeId()));
        okResponsesReceived.add(message.getSenderNodeId());
        // This indicates a higher-ID node is active and will take over.
        // This node should stop its own attempt to become leader if it initiated the election.
        // The logic in startElection handles waiting for these.
    }

    /**
     * Handles an incoming COORDINATOR message, announcing a new leader.
     * @param message The COORDINATOR message received.
     */
    public void handleCoordinatorMessage(ElectionMessage message) {
        int newLeaderId = message.getSenderNodeId();
        logger.info(String.format("Node %d: Received COORDINATOR message. New leader is Node %d.", selfNode.getNodeId(), newLeaderId));

        selfNode.setCurrentLeaderId(newLeaderId);
        if (newLeaderId == selfNode.getNodeId()) {
            selfNode.setLeader(true); // This node is the leader
        } else {
            selfNode.setLeader(false); // Another node is the leader
        }
        electionInProgress.set(false); // Election process is now complete
    }

    /**
     * This node declares itself as the leader and sends COORDINATOR messages to all other active peers.
     */
    private void announceAsLeader() {
        selfNode.setLeader(true); // Mark self as leader
        selfNode.setCurrentLeaderId(selfNode.getNodeId());
        logger.info(String.format("Node %d: Announcing self as LEADER to all peers.", selfNode.getNodeId()));

        ElectionMessage coordinatorMsg = new ElectionMessage(
                MessageType.COORDINATOR,
                selfNode.getNodeId(),
                -1, // Target ID -1 can signify broadcast or handle individually in sendMessageAsync
                selfNode.getLamportClock().getTime()); // Send current Lamport time

        for (EnhancedNode peer : selfNode.getPeers()) {
            if (peer.isActive() && !peer.isFailed()) {
                // Create a new message for each peer with specific targetNodeId
                ElectionMessage peerSpecificCoordinatorMsg = new ElectionMessage(
                        MessageType.COORDINATOR,
                        selfNode.getNodeId(),
                        peer.getNodeId(), // Specific target
                        selfNode.getLamportClock().getTime());
                messageHandler.sendMessageAsync(peer, peerSpecificCoordinatorMsg);
            }
        }
        // Also send to self to update its own state if necessary (though setLeader(true) handles it)
        // selfNode.receiveMessage(coordinatorMsg); // This might cause loop if not handled carefully
    }
}
