package assignment4.network;

import assignment4.model.ElectionMessage;
import assignment4.model.MessageType;
import assignment4.node.EnhancedNode;
import common.utils.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Handles sending and receiving (routing) of messages for an EnhancedNode.
 * Simulates network delay for messages.
 */
public class MessageHandler {
    private final EnhancedNode selfNode; // The node this handler belongs to
    private final Logger logger = Logger.getInstance();
    private final Random random = new Random(); // For simulating network delay
    // Executor service for sending messages asynchronously to avoid blocking the caller
    private final ExecutorService sendExecutor = Executors.newCachedThreadPool();


    /**
     * Constructor for MessageHandler.
     * @param selfNode The node instance this handler serves.
     */
    public MessageHandler(EnhancedNode selfNode) {
        this.selfNode = selfNode;
    }

    /**
     * Routes an incoming message to the appropriate handler method in the node or its components.
     * This method is typically called by the node's receiveMessage or an internal message queue.
     * @param message The message to be routed.
     */
    public void routeMessage(ElectionMessage message) {
        if (selfNode.isFailed() && message.getType() != MessageType.COORDINATOR) { // Allow failed node to learn new leader
            logger.debug(String.format("Node %d (failed): Dropping message %s", selfNode.getNodeId(), message.toString()));
            return;
        }
        if (!selfNode.isRunning() && message.getType() != MessageType.COORDINATOR) {
            logger.debug(String.format("Node %d (not running): Dropping message %s", selfNode.getNodeId(), message.toString()));
            return;
        }


        logger.debug(String.format("Node %d: Routing message: %s", selfNode.getNodeId(), message.toString()));

        switch (message.getType()) {
            case ELECTION:
                selfNode.getElectionAlgorithm().handleElectionMessage(message);
                break;
            case OK:
                selfNode.getElectionAlgorithm().handleOkMessage(message);
                break;
            case COORDINATOR:
                selfNode.getElectionAlgorithm().handleCoordinatorMessage(message);
                break;
            case TRANSACTION:
                if (selfNode.isLeader()) {
                    selfNode.addTransactionToQueue(message.getTransaction());
                } else {
                    // If not leader, this message might have been misrouted or leader changed.
                    // For now, log it. A more robust system might forward or re-queue.
                    logger.warn(String.format("Node %d (not leader): Received transaction message: %s. Current leader: %d",
                            selfNode.getNodeId(), message.toString(), selfNode.getCurrentLeaderNode() != null ? selfNode.getCurrentLeaderNode().getNodeId() : -1 ));
                    // Attempt to forward to current leader if known
                    EnhancedNode leader = selfNode.getCurrentLeaderNode();
                    if (leader != null && leader.getNodeId() != selfNode.getNodeId()) {
                        logger.info(String.format("Node %d: Forwarding transaction to perceived leader Node %d", selfNode.getNodeId(), leader.getNodeId()));
                        sendMessageAsync(leader, message);
                    } else if (leader == null) {
                        logger.warn(String.format("Node %d: No leader known to forward transaction. Initiating election.", selfNode.getNodeId()));
                        selfNode.getElectionAlgorithm().startElection();
                    }
                }
                break;
            case HEARTBEAT:
                selfNode.handleHeartbeatMessage(message);
                break;
            case ALIVE:
                selfNode.handleAliveMessage(message);
                break;
            // Add cases for other message types if necessary
            default:
                logger.warn(String.format("Node %d: Received message with unhandled type: %s", selfNode.getNodeId(), message.getType()));
        }
    }

    /**
     * Sends a message to a target node asynchronously.
     * Simulates network delay before the target node receives the message.
     * @param targetNode The node to send the message to.
     * @param message The message to send.
     */
    public void sendMessageAsync(EnhancedNode targetNode, ElectionMessage message) {
        if (targetNode == null) {
            logger.error(String.format("Node %d: Attempted to send message to a null target. Message: %s", selfNode.getNodeId(), message.toString()));
            return;
        }
        if (selfNode.isFailed()) {
            logger.debug(String.format("Node %d (failed): Cannot send message %s to Node %d.", selfNode.getNodeId(), message.getType(), targetNode.getNodeId()));
            return;
        }


        sendExecutor.submit(() -> {
            try {
                // Simulate network delay (e.g., 10-100 ms)
                int delay = 10 + random.nextInt(91);
                Thread.sleep(delay);

                if (targetNode.isActive() && !targetNode.isFailed()) { // Check if target is in a state to receive
                    logger.debug(String.format("Node %d: SENT %s to Node %d (delay: %dms)",
                            selfNode.getNodeId(), message.toString(), targetNode.getNodeId(), delay));
                    targetNode.receiveMessage(message);
                } else {
                    logger.warn(String.format("Node %d: Target Node %d is not active/failed. Message %s not delivered.",
                            selfNode.getNodeId(), targetNode.getNodeId(), message.toString()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(String.format("Node %d: Interrupted while sending message %s to Node %d.",
                        selfNode.getNodeId(), message.toString(), targetNode.getNodeId()));
            }
        });
    }

    /**
     * Shuts down the message handler's executor service.
     */
    public void shutdown() {
        logger.info(String.format("Node %d: Shutting down MessageHandler executor.", selfNode.getNodeId()));
        sendExecutor.shutdown();
        try {
            if (!sendExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                sendExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            sendExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
