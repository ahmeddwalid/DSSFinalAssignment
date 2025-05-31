package assignment4;

import common.utils.Logger;
import assignment4.node.EnhancedNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo class for Assignment 4: Enhanced Bully Algorithm with Lamport Clocks.
 *
 * This demo showcases:
 * - Leader election using the Bully Algorithm.
 * - Transaction processing ordered by Lamport timestamps via a priority queue on the leader.
 * - Transactions executed by the leader at fixed intervals (e.g., 500ms).
 * - Simulation of leader failure and automatic re-election and recovery.
 */
public class BullyAlgorithmDemo {
    private static final int NUM_NODES = 5; // Number of nodes in the system
    private static final int PROCESSING_INTERVAL_MS = 500; // Leader processes queue every 500ms
    private static final Logger logger = Logger.getInstance(); // Singleton logger instance

    // Helper constants for sleep times, matching potential definitions in EnhancedNode or for clarity
    private static final long HEARTBEAT_TIMEOUT_MS = 3000L; // Explicitly long
    private static final long ELECTION_RESPONSE_TIMEOUT_MS = 1500L; // Explicitly long


    public static void main(String[] args) {
        logger.setDebugEnabled(false); // Enable for verbose debug output from nodes/election
        logger.section("Assignment 4: Enhanced Bully Algorithm with Lamport Clocks Demo");

        // 1. Create Nodes
        List<EnhancedNode> nodes = createAndInitializeNodes();

        // 2. Start all nodes (triggers initial election)
        startAllNodes(nodes);
        logger.info("All nodes started. Waiting for initial leader election to settle...");
        sleep(4000); // Allow time for initial election and heartbeats to stabilize

        // 3. Display status after initial election
        displaySystemStatus(nodes, "Status After Initial Election");

        // 4. Test concurrent transaction submission and ordering
        testTransactionSubmissionAndOrdering(nodes);
        logger.info("Waiting for transactions to be processed by the leader...");
        sleep(5000); // Allow time for transactions to be created, sent, queued, and processed

        displaySystemStatus(nodes, "Status After Initial Transactions");

        // 5. Test leader failure and recovery
        testLeaderFailureAndRecovery(nodes);
        logger.info("Waiting for system to stabilize after leader recovery and potential re-election...");
        sleep(5000); // Allow time for recovery and further transactions

        // 6. Final system status
        displaySystemStatus(nodes, "Final System Status");

        // 7. Shutdown all nodes
        shutdownAllNodes(nodes);
        logger.section("Demo Complete");
    }

    /**
     * Creates nodes and initializes their peer connections.
     * @return A list of created EnhancedNode instances.
     */
    private static List<EnhancedNode> createAndInitializeNodes() {
        logger.info(String.format("Creating %d nodes...", NUM_NODES));
        List<EnhancedNode> nodes = new ArrayList<>();
        for (int i = 1; i <= NUM_NODES; i++) {
            // Node IDs are typically 1-based or have some other scheme
            EnhancedNode node = new EnhancedNode(i, PROCESSING_INTERVAL_MS);
            nodes.add(node);
            logger.info(String.format("Created Node %d.", i));
        }

        // Initialize network: make each node aware of all other nodes as peers
        logger.info("Initializing network connections (all-to-all peer awareness)...");
        for (EnhancedNode node : nodes) {
            node.setPeers(nodes); // Pass the complete list for peer setup
        }
        return nodes;
    }

    /**
     * Starts all nodes in the system. Each node will begin its operations,
     * including participating in leader election.
     * @param nodes The list of nodes to start.
     */
    private static void startAllNodes(List<EnhancedNode> nodes) {
        logger.info("Starting all nodes...");
        for (EnhancedNode node : nodes) {
            node.start(); // Start node operations (election, heartbeats, etc.)
        }
    }

    /**
     * Simulates concurrent transaction submissions from different nodes.
     * @param nodes The list of active nodes.
     */
    private static void testTransactionSubmissionAndOrdering(List<EnhancedNode> nodes) {
        logger.section("Testing Concurrent Transaction Submission");
        ExecutorService transactionSubmitter = Executors.newFixedThreadPool(NUM_NODES);

        // Node 1 (ID 1) creates a transaction
        transactionSubmitter.submit(() -> {
            sleep(100); // Stagger submission slightly
            if (nodes.get(0).isActive()) nodes.get(0).createTransaction("TXN from Node 1: Update Profile");
        });

        // Node 3 (ID 3) creates a transaction (potentially earlier Lamport if clock ticks slower initially)
        transactionSubmitter.submit(() -> {
            sleep(50);
            if (nodes.get(2).isActive()) nodes.get(2).createTransaction("TXN from Node 3: Post New Message");
        });

        // Node 2 (ID 2) creates a transaction
        transactionSubmitter.submit(() -> {
            sleep(150);
            if (nodes.get(1).isActive()) nodes.get(1).createTransaction("TXN from Node 2: Like Comment");
        });

        // Node 4 (ID 4) creates a transaction
        transactionSubmitter.submit(() -> {
            sleep(200);
            if (nodes.get(3).isActive()) nodes.get(3).createTransaction("TXN from Node 4: Share Content");
        });


        transactionSubmitter.shutdown();
        try {
            if (!transactionSubmitter.awaitTermination(10, TimeUnit.SECONDS)) {
                transactionSubmitter.shutdownNow();
            }
        } catch (InterruptedException e) {
            transactionSubmitter.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Simulates the failure of the current leader node and observes the system's recovery.
     * @param nodes The list of active nodes.
     */
    private static void testLeaderFailureAndRecovery(List<EnhancedNode> nodes) {
        logger.section("Testing Leader Failure and Recovery");

        EnhancedNode currentLeader = findCurrentLeader(nodes);
        if (currentLeader == null) {
            logger.warn("No leader found to simulate failure. An election might be ongoing or failed.");
            // Attempt to trigger an election from a node if no leader
            if (!nodes.isEmpty() && nodes.get(0).isActive()) {
                logger.info("Attempting to start election from Node 1 as no leader was found.");
                nodes.get(0).getElectionAlgorithm().startElection();
                sleep(3000); // Wait for election
                currentLeader = findCurrentLeader(nodes);
                if (currentLeader == null) {
                    logger.error("Still no leader after attempting election. Aborting failure test.");
                    return;
                }
            } else {
                logger.error("No active nodes to start an election. Aborting failure test.");
                return;
            }
        }

        logger.info(String.format("Current leader is Node %d. Simulating its failure...", currentLeader.getNodeId()));
        currentLeader.userSimulatedFailure(); // Use the method designed for demo failure

        logger.info("Waiting for leader failure to be detected and new election to complete...");
        // Corrected line: Cast the sum of longs to int for the sleep method
        sleep((int) (HEARTBEAT_TIMEOUT_MS + ELECTION_RESPONSE_TIMEOUT_MS + 2000L)); // Wait for heartbeat timeout + election timeout + buffer

        displaySystemStatus(nodes, "Status After Leader Failure & Re-election");

        logger.info("Submitting new transactions to test the new leader...");
        // Submit transactions from a couple of non-failed nodes
        nodes.stream().filter(n -> n.isActive() && !n.isFailed()).limit(2).forEach(node -> {
            node.createTransaction("Post-failure TXN from Node " + node.getNodeId());
        });
        sleep(3000); // Allow time for new transactions to be processed

        logger.info(String.format("Recovering the failed Node %d...", currentLeader.getNodeId()));
        currentLeader.recover(); // The failed node attempts to recover
        sleep(4000); // Allow time for the recovered node to rejoin and potentially trigger election if needed

        displaySystemStatus(nodes, "Status After Node Recovery");
    }


    /**
     * Finds the current leader among the list of nodes.
     * @param nodes The list of nodes.
     * @return The leader EnhancedNode, or null if no leader is found.
     */
    private static EnhancedNode findCurrentLeader(List<EnhancedNode> nodes) {
        for (EnhancedNode node : nodes) {
            if (node.isLeader() && node.isActive() && !node.isFailed()) {
                return node;
            }
        }
        // Fallback: check currentLeaderId cache if direct isLeader is not set by all
        for (EnhancedNode node : nodes) {
            EnhancedNode leaderCandidate = node.getCurrentLeaderNode();
            if (leaderCandidate != null && leaderCandidate.isLeader() && leaderCandidate.isActive() && !leaderCandidate.isFailed()) {
                return leaderCandidate;
            }
        }
        return null;
    }

    /**
     * Displays the status of all nodes in the system.
     * @param nodes The list of nodes.
     * @param context A string describing the context of the status display.
     */
    private static void displaySystemStatus(List<EnhancedNode> nodes, String context) {
        logger.section(context);
        for (EnhancedNode node : nodes) {
            node.printStatus(); // Each node prints its own detailed status
        }
        EnhancedNode leader = findCurrentLeader(nodes);
        if (leader != null) {
            logger.info(String.format("VERIFIED LEADER: Node %d", leader.getNodeId()));
        } else {
            logger.warn("VERIFIED LEADER: No active leader found in the system.");
        }
    }

    /**
     * Shuts down all nodes gracefully.
     * @param nodes The list of nodes to shut down.
     */
    private static void shutdownAllNodes(List<EnhancedNode> nodes) {
        logger.section("Shutting Down All Nodes");
        for (EnhancedNode node : nodes) {
            node.shutdown();
        }
        // Wait for a moment for shutdown messages to clear
        sleep(1000);
    }

    /**
     * Utility method to pause execution.
     * @param milliseconds The duration to sleep in milliseconds.
     */
    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Sleep interrupted: " + e.getMessage());
        }
    }
}
