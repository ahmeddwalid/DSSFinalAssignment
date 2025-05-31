package assignment3;

import assignment3.model.Transaction;
import assignment3.node.BankNode;
// import assignment3.network.NetworkSimulator; // Optional, if used
import common.utils.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo class for Assignment 3: Gossip Protocol for Eventually Consistent Banking System.
 *
 * This demo simulates a set of bank nodes that use a gossip protocol
 * to propagate transactions. The goal is to observe eventual consistency
 * where all nodes converge to the same account balances over time.
 */
public class GossipBankingDemo { // This should be the only public class in this file
    private static final int NUM_NODES = 5; // Number of bank nodes in the system
    private static final Logger logger = Logger.getInstance();

    public static void main(String[] args) {
        logger.setDebugEnabled(false); // Set to true for verbose gossip/processing logs
        logger.section("Assignment 3: Gossip Protocol Banking System Demo");

        // 1. Create Bank Nodes
        List<BankNode> nodes = new ArrayList<>();
        for (int i = 1; i <= NUM_NODES; i++) {
            nodes.add(new BankNode(i));
        }

        // 2. Initialize Network (let each node know about all other nodes)
        //    and initialize their gossip protocols.
        logger.info("Initializing node peer connections and gossip protocols...");
        for (BankNode node : nodes) {
            // Each node gets a list of all nodes (will filter self in setPeers)
            node.setPeers(new ArrayList<>(nodes));
        }

        // (Optional) Initialize and start a NetworkSimulator if you are using one
        // NetworkSimulator networkSimulator = new NetworkSimulator(nodes);
        // networkSimulator.start();

        // 3. Start all bank nodes
        logger.info("Starting all bank nodes...");
        for (BankNode node : nodes) {
            node.start();
        }

        // 4. Display initial state of all nodes
        displayAllNodeStatuses(nodes, "Initial Node States");

        // 5. Test Scenario: Send different updates from two different nodes
        logger.section("Test Scenario: Concurrent Transactions from Different Nodes");
        // Use an ExecutorService to submit transactions concurrently
        ExecutorService transactionExecutor = Executors.newFixedThreadPool(2);
        // Using a latch to ensure all transaction creation calls are made before proceeding too quickly
        CountDownLatch transactionsLatch = new CountDownLatch(4); // For 4 transactions total

        // Node 1 creates transactions
        transactionExecutor.submit(() -> {
            try {
                if (nodes.get(0).isActive()) {
                    nodes.get(0).createTransaction(Transaction.Type.DEPOSIT, 500.0, "ACC001");
                    transactionsLatch.countDown();
                    sleep(100); // Slight delay between transactions from the same node
                    nodes.get(0).createTransaction(Transaction.Type.WITHDRAWAL, 100.0, "ACC002");
                    transactionsLatch.countDown();
                }
            } catch (Exception e) {
                logger.error("Error in Node 1 transaction thread: " + e.getMessage());
                // Ensure latch is counted down even on error to prevent deadlock
                while (transactionsLatch.getCount() > 2) transactionsLatch.countDown(); // Approximate for this thread
            }
        });

        // Node 3 (index 2) creates transactions
        transactionExecutor.submit(() -> {
            try {
                sleep(50); // Start slightly after Node 1
                if (nodes.get(2).isActive()) {
                    nodes.get(2).createTransaction(Transaction.Type.DEPOSIT, 300.0, "ACC002");
                    transactionsLatch.countDown();
                    sleep(100);
                    nodes.get(2).createTransaction(Transaction.Type.WITHDRAWAL, 50.0, "ACC001");
                    transactionsLatch.countDown();
                }
            } catch (Exception e) {
                logger.error("Error in Node 3 transaction thread: " + e.getMessage());
                while (transactionsLatch.getCount() > 0 && transactionsLatch.getCount() <= 2) transactionsLatch.countDown(); // Approximate
            }
        });

        // Wait for all transactions to be initiated
        try {
            if (!transactionsLatch.await(10, TimeUnit.SECONDS)) { // Increased timeout
                logger.warn("Timeout waiting for all transactions to be initiated by threads.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for transactions to be initiated.");
        }
        transactionExecutor.shutdown();


        // 6. Wait for gossip propagation and processing
        logger.info("Waiting for gossip propagation and eventual consistency (e.g., 8-10 seconds)...");
        sleep(10000); // Adjusted wait time

        // 7. Display final state of all nodes
        displayAllNodeStatuses(nodes, "Final Node States (After Gossip)");

        // 8. Verify eventual consistency (basic check)
        logger.section("Consistency Check");
        verifyConsistency(nodes);


        // 9. Shutdown all nodes (and network simulator if used)
        logger.section("Shutting Down System");
        for (BankNode node : nodes) {
            node.shutdown();
        }
        // if (networkSimulator != null) networkSimulator.stop();

        logger.info("Gossip Banking Demo complete.");
    }

    /**
     * Helper method to display the status of all nodes.
     * @param nodes List of BankNodes.
     * @param context A string describing the context of this status display.
     */
    private static void displayAllNodeStatuses(List<BankNode> nodes, String context) {
        logger.section(context);
        for (BankNode node : nodes) {
            if (node != null) { // Added null check
                node.printStatus();
                System.out.println("--------------------");
            }
        }
    }

    /**
     * Basic verification of eventual consistency.
     * Note: This is a simplified check. True verification is more complex.
     */
    private static void verifyConsistency(List<BankNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            logger.warn("Consistency Check: No nodes to check.");
            return;
        }

        logger.info("Performing simplified consistency check...");
        // For this demo, a full programmatic check is complex as it requires
        // nodes to expose detailed state (all account balances, all processed txn IDs)
        // in a comparable format. The printStatus() method provides human-readable output.
        // A true check would compare these data structures across all active nodes.

        // Example of what a more detailed check might start to look like:
        // (Requires BankNode to have methods like `getAccountBalancesSnapshot()` and `getProcessedTransactionIdsSnapshot()`)
        /*
        boolean allConsistent = true;
        Map<String, Double> referenceBalances = null;
        Set<String> referenceTxIds = null;

        if (nodes.get(0) != null && nodes.get(0).isActive()) {
            // referenceBalances = nodes.get(0).getAccountBalancesSnapshot();
            // referenceTxIds = nodes.get(0).getProcessedTransactionIdsSnapshot();
        } else {
            logger.warn("Consistency Check: Node 0 is not available as a reference.");
            allConsistent = false; // Cannot establish a baseline
        }

        if (allConsistent && referenceBalances != null && referenceTxIds != null) {
            for (int i = 1; i < nodes.size(); i++) {
                BankNode currentNode = nodes.get(i);
                if (currentNode == null || !currentNode.isActive()) continue;

                // Map<String, Double> currentBalances = currentNode.getAccountBalancesSnapshot();
                // Set<String> currentTxIds = currentNode.getProcessedTransactionIdsSnapshot();

                // if (!referenceBalances.equals(currentBalances)) {
                //     logger.error(String.format("Node %d balances inconsistent with reference Node 0.", currentNode.getNodeId()));
                //     allConsistent = false;
                // }
                // if (!referenceTxIds.equals(currentTxIds)) {
                //     logger.error(String.format("Node %d processed transaction IDs inconsistent with reference Node 0.", currentNode.getNodeId()));
                //     allConsistent = false;
                // }
            }
        }

        if (allConsistent) {
            logger.info("Consistency Check: System appears to have reached eventual consistency (based on available data).");
        } else {
            logger.error("Consistency Check: Inconsistency detected or unable to verify fully.");
        }
        */
        logger.info("A full consistency check would compare all account states and processed transaction sets across nodes.");
        logger.info("Please visually inspect the 'Final Node States' logs for convergence.");
        logger.info("Reminder: Eventual consistency means states converge over time. Delays in gossip or processing can affect convergence speed.");
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
