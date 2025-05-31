package assignment3.network;

import assignment3.model.GossipMessage;
import assignment3.node.BankNode;
import common.utils.Logger;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Simulates a network layer for message passing between BankNodes.
 * This can introduce delays, message loss (optional), etc.
 * For this assignment, it's primarily a conceptual layer; actual message passing
 * might be done directly or via a shared executor in GossipProtocol/BankNode.
 *
 * If used, this class would centralize message sending logic.
 * However, the current structure has BankNode/GossipProtocol handle async sends.
 * This file can serve as a placeholder or be expanded if a more distinct network layer is desired.
 */
public class NetworkSimulator {
    private final List<BankNode> allNodes; // All nodes in the simulated network
    private final Logger logger = Logger.getInstance();
    private final Random random = new Random();
    private final ExecutorService networkExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private boolean isRunning = false;

    /**
     * Constructor for NetworkSimulator.
     * @param allNodes A list of all BankNodes participating in the network.
     */
    public NetworkSimulator(List<BankNode> allNodes) {
        this.allNodes = allNodes;
    }

    /**
     * Starts the network simulator.
     */
    public void start() {
        isRunning = true;
        logger.info("NetworkSimulator: Started.");
    }

    /**
     * Stops the network simulator and shuts down its executor.
     */
    public void stop() {
        isRunning = false;
        logger.info("NetworkSimulator: Stopping...");
        networkExecutor.shutdown();
        try {
            if (!networkExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                networkExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            networkExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("NetworkSimulator: Stopped.");
    }

    /**
     * Sends a gossip message from a source node to a target node with simulated delay.
     * @param sourceNode The node sending the message.
     * @param targetNode The node to receive the message.
     * @param message The GossipMessage to send.
     */
    public void sendMessage(BankNode sourceNode, BankNode targetNode, GossipMessage message) {
        if (!isRunning) {
            logger.warn(String.format("NetworkSimulator: Not running. Cannot send message from Node %d to Node %d.",
                    sourceNode.getNodeId(), targetNode.getNodeId()));
            return;
        }

        if (sourceNode == null || targetNode == null || message == null) {
            logger.error("NetworkSimulator: Null parameter in sendMessage. Aborting send.");
            return;
        }

        networkExecutor.submit(() -> {
            try {
                // Simulate network delay
                int delayMs = 50 + random.nextInt(150); // e.g., 50-200ms delay
                Thread.sleep(delayMs);

                logger.debug(String.format("NetworkSimulator: Delivering message from Node %d to Node %d (Transaction: %s, Hops: %d) with delay %dms.",
                        sourceNode.getNodeId(), targetNode.getNodeId(), message.getTransaction().getId(), message.getHopCount(), delayMs));

                // Deliver the message to the target node's receiveGossip method
                targetNode.receiveGossip(message);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(String.format("NetworkSimulator: Message delivery from Node %d to Node %d interrupted.",
                        sourceNode.getNodeId(), targetNode.getNodeId()));
            }
        });
    }

    // Additional methods could be added here for:
    // - Broadcasting messages
    // - Simulating network partitions
    // - Simulating message loss or corruption (though not required by assignment)
}
