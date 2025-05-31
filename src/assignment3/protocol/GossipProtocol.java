package assignment3.protocol;

import assignment3.model.GossipMessage;
import assignment3.model.Transaction;
import assignment3.node.BankNode; // Assuming BankNode is the node type
import common.interfaces.Protocol;
import common.utils.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Implements the Gossip Protocol for disseminating transactions
 * among BankNodes in an eventually consistent manner.
 */
public class GossipProtocol implements Protocol {
    private final BankNode selfNode; // The node this protocol instance belongs to
    private final List<BankNode> allPeers; // All other nodes in the network
    private final Logger logger = Logger.getInstance();
    private final Random random = new Random();
    private final ExecutorService messageExecutor; // For sending gossip messages asynchronously
    private volatile boolean isRunning = false;

    private static final int GOSSIP_TARGET_COUNT = 2; // Number of random peers to gossip to
    private static final int MAX_GOSSIP_HOPS = 5; // To prevent messages from circulating indefinitely

    /**
     * Constructor for GossipProtocol.
     * @param selfNode The BankNode instance this protocol serves.
     * @param allPeers A list of all other BankNodes in the system.
     * @param messageExecutor Executor service for asynchronous message sending.
     */
    public GossipProtocol(BankNode selfNode, List<BankNode> allPeers, ExecutorService messageExecutor) {
        this.selfNode = selfNode;
        this.allPeers = allPeers; // Should be a list of other nodes, not including selfNode
        this.messageExecutor = messageExecutor;
    }

    @Override
    public void initialize() {
        logger.info(String.format("Node %d: GossipProtocol initializing.", selfNode.getNodeId()));
        // Initialization logic, if any (e.g., setting up connections, although here peers are passed in)
    }

    @Override
    public void start() {
        isRunning = true;
        logger.info(String.format("Node %d: GossipProtocol started.", selfNode.getNodeId()));
        // Start any periodic tasks, e.g., anti-entropy, if implemented.
        // For basic gossip dissemination, starting might just mean it's ready to send/receive.
    }

    @Override
    public void stop() {
        isRunning = false;
        logger.info(String.format("Node %d: GossipProtocol stopping.", selfNode.getNodeId()));
        // Clean up resources, stop periodic tasks.
    }

    @Override
    public String getProtocolName() {
        return "GossipProtocol";
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Initiates the gossip process for a newly created transaction.
     * The transaction is wrapped in a GossipMessage and sent to a few random peers.
     * @param transaction The transaction to be gossiped.
     */
    public void initiateGossip(Transaction transaction) {
        if (!isRunning) {
            logger.warn(String.format("Node %d: GossipProtocol not running, cannot initiate gossip for %s.", selfNode.getNodeId(), transaction.getId()));
            return;
        }
        logger.info(String.format("Node %d: Initiating gossip for %s", selfNode.getNodeId(), transaction.toString()));
        GossipMessage initialMessage = new GossipMessage(transaction, selfNode.getNodeId(), 0); // Hop count starts at 0
        propagateGossip(initialMessage);
    }

    /**
     * Continues gossiping a received transaction to other peers.
     * This is called when a node receives a transaction from another node and decides to propagate it further.
     * @param receivedGossipMessage The GossipMessage that was received and processed.
     */
    public void continueGossip(GossipMessage receivedGossipMessage) {
        if (!isRunning) {
            logger.warn(String.format("Node %d: GossipProtocol not running, cannot continue gossip for %s.", selfNode.getNodeId(), receivedGossipMessage.getTransaction().getId()));
            return;
        }

        // Increment hop count and update sender for the new message
        GossipMessage messageToForward = receivedGossipMessage.forward(selfNode.getNodeId());

        if (messageToForward.shouldDrop() || messageToForward.getHopCount() > MAX_GOSSIP_HOPS) { // Check TTL
            logger.debug(String.format("Node %d: Dropping gossip message for %s due to max hops (%d) exceeded.",
                    selfNode.getNodeId(), messageToForward.getTransaction().getId(), messageToForward.getHopCount()));
            return;
        }

        logger.debug(String.format("Node %d: Continuing gossip for %s (hop %d)",
                selfNode.getNodeId(), messageToForward.getTransaction().toString(), messageToForward.getHopCount()));
        propagateGossip(messageToForward);
    }

    /**
     * Selects random peers and sends the gossip message to them.
     * @param gossipMessage The message to propagate.
     */
    private void propagateGossip(GossipMessage gossipMessage) {
        if (allPeers.isEmpty()) {
            logger.debug(String.format("Node %d: No peers to gossip to for transaction %s.", selfNode.getNodeId(), gossipMessage.getTransaction().getId()));
            return;
        }

        List<BankNode> eligiblePeers = new ArrayList<>(allPeers);
        // Optional: Don't send back to the original sender of this specific message instance,
        // though with random selection and multiple paths, it's less of an issue.
        // eligiblePeers.removeIf(peer -> peer.getNodeId() == gossipMessage.getSenderNodeId()); // Basic loop avoidance

        if (eligiblePeers.isEmpty()) {
            logger.debug(String.format("Node %d: No eligible peers (excluding sender) to gossip to for transaction %s.", selfNode.getNodeId(), gossipMessage.getTransaction().getId()));
            return;
        }

        Collections.shuffle(eligiblePeers, random); // Shuffle for random selection

        int count = 0;
        for (int i = 0; i < eligiblePeers.size() && count < GOSSIP_TARGET_COUNT; i++) {
            BankNode targetPeer = eligiblePeers.get(i);
            if (targetPeer.getNodeId() == gossipMessage.getTransaction().getSourceNodeId() && gossipMessage.getHopCount() > 0) {
                // Avoid sending back to the original source node immediately after it sent, unless it's the first hop.
                // This is a simple optimization. More complex schemes exist.
                // continue;
            }

            // Send asynchronously
            final BankNode finalTargetPeer = targetPeer; // Effectively final for lambda
            messageExecutor.submit(() -> {
                try {
                    // Simulate network delay
                    Thread.sleep(50 + random.nextInt(100)); // 50-150ms delay
                    logger.debug(String.format("Node %d: Gossiping %s to Node %d (Hop: %d)",
                            selfNode.getNodeId(), gossipMessage.getTransaction().getId(), finalTargetPeer.getNodeId(), gossipMessage.getHopCount()));
                    finalTargetPeer.receiveGossip(gossipMessage); // Target node receives the gossip
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error(String.format("Node %d: Interrupted while gossiping to Node %d.", selfNode.getNodeId(), finalTargetPeer.getNodeId()));
                }
            });
            count++;
        }
        if (count == 0 && !eligiblePeers.isEmpty()) {
            logger.debug(String.format("Node %d: Did not select any peers for transaction %s (e.g. all were source).", selfNode.getNodeId(), gossipMessage.getTransaction().getId()));
        }
    }
}
