package assignment4.node;

import assignment4.model.ElectionMessage;
import assignment4.model.MessageType;
import assignment4.model.TimestampedTransaction;
import assignment4.election.BullyElection;
import assignment4.clock.LamportClock;
import assignment4.network.MessageHandler;
import common.interfaces.Node; // Using the more complete common.interfaces.Node
import common.utils.Logger;

import java.util.Set;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * Represents an enhanced node in the distributed system.
 * Implements the Bully Algorithm for leader election and uses Lamport Clocks
 * for ordering transactions. The leader processes transactions from a priority queue
 * at fixed intervals.
 */
public class EnhancedNode implements Node {
    private final int nodeId;
    private final LamportClock lamportClock;
    private final BullyElection electionAlgorithm;
    private final MessageHandler messageHandler;
    private final Logger logger = Logger.getInstance();

    // Node state
    private volatile boolean isLeader = false;
    private volatile boolean isActive = true; // For the Node interface; similar to !isFailed
    private volatile boolean isFailed = false; // Specific failure state for bully algorithm simulation
    private volatile boolean isRunning = false; // Indicates if the node's services are active
    private volatile int currentLeaderId = -1; // Cached ID of the current leader

    // Leader-specific fields
    private final PriorityQueue<TimestampedTransaction> transactionQueue;
    private final ReentrantLock queueLock = new ReentrantLock(); // To protect access to the transaction queue

    // Network and processing
    private final int processingIntervalMs;
    private final ScheduledExecutorService scheduler; // For periodic tasks like transaction processing and heartbeats
    private final ExecutorService messageExecutor;    // For handling incoming messages asynchronously

    // Peers and statistics
    private final Set<EnhancedNode> peers = ConcurrentHashMap.newKeySet(); // Thread-safe set of known peers
    private final AtomicInteger processedTransactionCount = new AtomicInteger(0);

    // Heartbeat tracking (for non-leader nodes to detect leader failure)
    private volatile long lastHeartbeatFromLeaderTime = 0;
    private static final long HEARTBEAT_TIMEOUT_MS = 3000; // If no heartbeat from leader in this time, assume failure


    /**
     * Constructor for EnhancedNode.
     * @param nodeId The unique ID for this node.
     * @param processingIntervalMs The interval (in milliseconds) at which the leader processes transactions.
     */
    public EnhancedNode(int nodeId, int processingIntervalMs) {
        this.nodeId = nodeId;
        this.processingIntervalMs = processingIntervalMs;
        this.lamportClock = new LamportClock();
        this.messageHandler = new MessageHandler(this); // MessageHandler needs a reference to this node
        this.electionAlgorithm = new BullyElection(this, messageHandler); // Election algorithm also needs this node

        this.transactionQueue = new PriorityQueue<>(); // Transactions ordered by Lamport timestamp
        this.scheduler = Executors.newScheduledThreadPool(2); // For leader's transaction processing and heartbeats
        this.messageExecutor = Executors.newCachedThreadPool(); // For handling incoming messages
    }

    /**
     * Adds a peer node to this node's list of known peers.
     * @param peer The peer node to add.
     */
    public void addPeer(EnhancedNode peer) {
        if (peer != null && peer.getNodeId() != this.nodeId) {
            this.peers.add(peer);
        }
    }

    public void setPeers(List<EnhancedNode> allNodes) {
        this.peers.clear();
        for (EnhancedNode node : allNodes) {
            if (node.getNodeId() != this.nodeId) {
                this.peers.add(node);
            }
        }
    }


    @Override
    public void start() {
        if (isRunning) {
            logger.warn(String.format("Node %d: Already running.", nodeId));
            return;
        }
        isRunning = true;
        isFailed = false;
        isActive = true;
        currentLeaderId = -1; // Reset leader knowledge
        logger.info(String.format("Node %d: Starting...", nodeId));

        // Schedule leader's transaction processing task
        scheduler.scheduleAtFixedRate(this::processTransactionsFromQueue,
                processingIntervalMs, processingIntervalMs, TimeUnit.MILLISECONDS);

        // Schedule heartbeat mechanism
        scheduler.scheduleAtFixedRate(this::manageHeartbeats,
                1000, 1000, TimeUnit.MILLISECONDS); // e.g., every 1 second

        // Initiate leader election upon startup
        // Run in a separate thread to not block the start method
        CompletableFuture.runAsync(electionAlgorithm::startElection, messageExecutor);
        logger.info(String.format("Node %d: Started. Initial election process triggered.", nodeId));
    }

    /**
     * Creates a new transaction with the current Lamport timestamp and submits it.
     * @param operation A string describing the transaction.
     */
    public void createTransaction(String operation) {
        if (isFailed || !isRunning) {
            logger.warn(String.format("Node %d: Cannot create transaction, node is failed or not running.", nodeId));
            return;
        }

        lamportClock.increment(); // Increment clock for the local event of creating a transaction
        TimestampedTransaction transaction = new TimestampedTransaction(
                operation, this.nodeId, lamportClock.getTime());

        logger.info(String.format("Node %d: Created %s", nodeId, transaction.toString()));
        sendTransactionToIdentifiedLeader(transaction);
    }

    private void sendTransactionToIdentifiedLeader(TimestampedTransaction transaction) {
        if (isLeader && !isFailed) {
            // If this node is the leader, add directly to its queue
            addTransactionToQueue(transaction);
        } else {
            // Otherwise, send to the known leader
            EnhancedNode leader = getCurrentLeaderNode();
            if (leader != null && leader.isActive() && !leader.isFailed()) {
                logger.debug(String.format("Node %d: Sending transaction %s to Leader Node %d", nodeId, transaction.getTransactionId(), leader.getNodeId()));
                ElectionMessage transactionMsg = new ElectionMessage(
                        MessageType.TRANSACTION,
                        this.nodeId,
                        leader.getNodeId(),
                        lamportClock.getTime(), // Include sender's current Lamport time
                        transaction);
                messageHandler.sendMessageAsync(leader, transactionMsg);
            } else {
                logger.warn(String.format("Node %d: No active leader known to send transaction %s. Election might be needed.", nodeId, transaction.getTransactionId()));
                // Optionally, queue locally or trigger election if no leader is known for too long
                electionAlgorithm.startElection(); // If no leader, try to elect one
            }
        }
    }

    /**
     * Receives a message from another node.
     * This method is typically called by a network simulation layer or MessageHandler.
     * @param message The message received.
     */
    public void receiveMessage(ElectionMessage message) {
        if (isFailed && message.getType() != MessageType.COORDINATOR) { // Allow failed node to learn about new leader
            logger.debug(String.format("Node %d: Failed, ignoring message: %s", nodeId, message.toString()));
            return;
        }
        if (!isRunning && message.getType() != MessageType.COORDINATOR) {
            logger.debug(String.format("Node %d: Not running, ignoring message: %s", nodeId, message.toString()));
            return;
        }

        // Update Lamport clock based on the timestamp in the message
        // This rule is crucial: max(local_clock, message_clock) + 1
        // The messageTimestamp in ElectionMessage should be the sender's Lamport clock time.
        lamportClock.update(message.getMessageTimestamp());
        logger.debug(String.format("Node %d: Received %s. Lamport Clock updated to %d.",
                nodeId, message.toString(), lamportClock.getTime()));

        // Handle the message asynchronously
        messageExecutor.submit(() -> messageHandler.routeMessage(message));
    }


    /**
     * Adds a transaction to the leader's priority queue.
     * This method should only be called on the leader node.
     * @param transaction The transaction to add.
     */
    public void addTransactionToQueue(TimestampedTransaction transaction) {
        if (!isLeader) {
            logger.warn(String.format("Node %d: Not leader, cannot add to transaction queue: %s", nodeId, transaction.toString()));
            // Forward to leader if known, or handle error
            sendTransactionToIdentifiedLeader(transaction); // Attempt to forward
            return;
        }
        if (isFailed) {
            logger.warn(String.format("Node %d: Leader is marked as failed, cannot queue transaction: %s", nodeId, transaction.toString()));
            return;
        }

        queueLock.lock();
        try {
            transactionQueue.offer(transaction);
            logger.info(String.format("Leader Node %d: Queued %s. Queue size: %d",
                    nodeId, transaction.toString(), transactionQueue.size()));
        } finally {
            queueLock.unlock();
        }
    }

    /**
     * Periodically called on the leader node to process transactions from its queue.
     */
    private void processTransactionsFromQueue() {
        if (!isLeader || isFailed || !isRunning) {
            return; // Only the active, non-failed leader processes transactions
        }

        queueLock.lock();
        try {
            if (!transactionQueue.isEmpty()) {
                TimestampedTransaction transaction = transactionQueue.poll(); // Get transaction with the smallest timestamp
                if (transaction != null) {
                    executeTransaction(transaction);
                }
            }
        } finally {
            queueLock.unlock();
        }
    }

    private void executeTransaction(TimestampedTransaction transaction) {
        // Increment clock for the local event of executing a transaction
        lamportClock.increment();
        processedTransactionCount.incrementAndGet();

        logger.info(String.format("Leader Node %d: EXECUTED %s. Lamport Clock: %d. (Total processed: %d)",
                nodeId, transaction.toString(), lamportClock.getTime(), processedTransactionCount.get()));

        // Simulate transaction processing time
        try {
            Thread.sleep(50); // Small delay to simulate work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(String.format("Node %d: Execution of transaction %s interrupted.", nodeId, transaction.getTransactionId()));
        }
        // In a real system, the leader would then multicast the executed transaction (or its result)
        // to other nodes to ensure state consistency, along with its Lamport timestamp.
        // For this assignment, the focus is on ordered execution by the leader.
    }

    private void manageHeartbeats() {
        if (isFailed || !isRunning) return;

        if (isLeader) {
            // Leader sends heartbeats to all peers
            logger.debug(String.format("Leader Node %d: Sending heartbeats.", nodeId));
            ElectionMessage heartbeatMsg = new ElectionMessage(
                    MessageType.HEARTBEAT,
                    nodeId,
                    -1, // Target ID -1 can signify broadcast or handle individually
                    lamportClock.getTime()
            );
            for (EnhancedNode peer : peers) {
                if (peer.isActive() && !peer.isFailed()) {
                    messageHandler.sendMessageAsync(peer, new ElectionMessage(MessageType.HEARTBEAT, nodeId, peer.getNodeId(), lamportClock.getTime()));
                }
            }
        } else {
            // Non-leader nodes check for leader heartbeats
            if (currentLeaderId != -1 && lastHeartbeatFromLeaderTime > 0 &&
                    (System.currentTimeMillis() - lastHeartbeatFromLeaderTime) > HEARTBEAT_TIMEOUT_MS) {
                logger.warn(String.format("Node %d: Leader Node %d missed heartbeat. Assuming failure. Starting election.", nodeId, currentLeaderId));
                currentLeaderId = -1; // Clear current leader
                lastHeartbeatFromLeaderTime = 0;
                electionAlgorithm.startElection();
            } else if (currentLeaderId == -1 && isRunning && !isFailed) {
                // If no leader is known, and we are active, periodically try to start an election.
                // This handles scenarios where a node starts up and no leader announces itself.
                logger.info(String.format("Node %d: No leader known. Proactively starting election.", nodeId));
                electionAlgorithm.startElection();
            }
        }
    }

    public void handleHeartbeatMessage(ElectionMessage message) {
        if (message.getSenderNodeId() == currentLeaderId) {
            lastHeartbeatFromLeaderTime = System.currentTimeMillis();
            logger.debug(String.format("Node %d: Received heartbeat from Leader Node %d.", nodeId, message.getSenderNodeId()));
            // Optionally, send an ALIVE message back to the leader
            ElectionMessage aliveMsg = new ElectionMessage(MessageType.ALIVE, nodeId, message.getSenderNodeId(), lamportClock.getTime());
            EnhancedNode leaderNode = findNodeById(message.getSenderNodeId());
            if(leaderNode != null) messageHandler.sendMessageAsync(leaderNode, aliveMsg);

        } else if (message.getSenderNodeId() != nodeId) { // A non-leader sent a heartbeat, which is unusual
            logger.warn(String.format("Node %d: Received heartbeat from non-leader Node %d. Current leader is %d.", nodeId, message.getSenderNodeId(), currentLeaderId));
        }
    }
    public void handleAliveMessage(ElectionMessage message) {
        if (isLeader) {
            logger.debug(String.format("Leader Node %d: Received ALIVE confirmation from Node %d.", nodeId, message.getSenderNodeId()));
            // Leader can use this to maintain a list of active followers.
        }
    }


    public void setCurrentLeaderId(int leaderId) {
        if (this.currentLeaderId != leaderId) {
            logger.info(String.format("Node %d: New leader identified: Node %d.", nodeId, leaderId));
            this.currentLeaderId = leaderId;
            if (leaderId != this.nodeId) {
                this.isLeader = false; // Ensure this node is not marked as leader if another is chosen
            }
            this.lastHeartbeatFromLeaderTime = System.currentTimeMillis(); // Reset heartbeat timer on new leader
        }
    }

    public EnhancedNode getCurrentLeaderNode() {
        if (currentLeaderId == -1) return null;
        if (currentLeaderId == nodeId) return this;
        return findNodeById(currentLeaderId);
    }

    public EnhancedNode findNodeById(int id) {
        if (id == this.nodeId) return this;
        for (EnhancedNode peer : peers) {
            if (peer.getNodeId() == id) {
                return peer;
            }
        }
        logger.warn(String.format("Node %d: Could not find peer with ID %d.", nodeId, id));
        return null; // Peer not found
    }


    @Override
    public void fail() {
        logger.warn(String.format("Node %d: SIMULATING FAILURE.", nodeId));
        this.isFailed = true;
        this.isActive = false;
        this.isLeader = false; // A failed node cannot be a leader
        this.isRunning = false; // Stop processing tasks

        // Clear transaction queue if it was a leader
        queueLock.lock();
        try {
            transactionQueue.clear();
        } finally {
            queueLock.unlock();
        }
        // Note: Schedulers are not shut down here, as 'recover' might restart them.
        // If fail is permanent, shutdown() should be called.
    }

    // This is the simulateFailure from the demo
    public void userSimulatedFailure() {
        fail(); // Call the internal fail method
    }


    @Override
    public void recover() {
        if (!isFailed) {
            logger.info(String.format("Node %d: Already active, no recovery needed.", nodeId));
            return;
        }
        logger.info(String.format("Node %d: RECOVERING...", nodeId));
        this.isFailed = false;
        this.isActive = true;
        // this.isRunning = true; // Set by start()
        // Restart services and participate in election
        // Re-initialize necessary state, then call start() or a similar method.
        // For simplicity, we assume recovery means it can rejoin.
        // It should try to find the current leader or start an election.
        start(); // Re-start the node's operations, including election participation
    }


    @Override
    public void shutdown() {
        if (!isRunning) {
            // logger.info(String.format("Node %d: Already shut down.", nodeId));
            // return;
        }
        logger.info(String.format("Node %d: Shutting down...", nodeId));
        isRunning = false;
        isActive = false;
        isFailed = true; // Consider a shutdown node as failed for election purposes

        scheduler.shutdown();
        messageExecutor.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!messageExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                messageExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error(String.format("Node %d: Interrupted during shutdown. Forcing shutdown of executors.", nodeId));
            scheduler.shutdownNow();
            messageExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info(String.format("Node %d: Shutdown complete.", nodeId));
    }

    @Override
    public void printStatus() {
        logger.info(String.format(
                "Node %d Status: [Active: %s, Failed: %s, Running: %s, Leader: %s, Known Leader ID: %d, Lamport Clock: %d, Processed Tx: %d, Queue Size: %d]",
                nodeId, isActive, isFailed, isRunning, isLeader, currentLeaderId, lamportClock.getTime(),
                processedTransactionCount.get(), getQueueSize()));
    }

    // Getters
    @Override
    public int getNodeId() { return nodeId; }
    public LamportClock getLamportClock() { return lamportClock; }
    public BullyElection getElectionAlgorithm() { return electionAlgorithm; }
    public Set<EnhancedNode> getPeers() { return peers; }
    public boolean isLeader() { return isLeader; }

    @Override
    public boolean isActive() { return isActive && !isFailed && isRunning; } // More comprehensive active check

    public boolean isFailed() { return isFailed; }
    public boolean isRunning() { return isRunning; } // From user's demo
    public int getProcessedTransactionCount() { return processedTransactionCount.get(); } // From user's demo

    public int getQueueSize() {
        queueLock.lock();
        try {
            return transactionQueue.size();
        } finally {
            queueLock.unlock();
        }
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
        if (isLeader) {
            this.currentLeaderId = this.nodeId; // If I become leader, I am the current leader
            logger.info(String.format("Node %d: Has become the LEADER.", nodeId));
        } else {
            // If stepping down, currentLeaderId should be updated by a COORDINATOR message from new leader
            // or set to -1 if no leader is known.
            logger.info(String.format("Node %d: Stepped down as leader.", nodeId));
        }
    }
}
