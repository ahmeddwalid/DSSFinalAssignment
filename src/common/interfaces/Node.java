package common.interfaces;

/**
 * Base interface for all distributed system nodes
 * Provides common functionality for node identification and lifecycle management
 */
public interface Node {

    /**
     * Get the unique identifier for this node
     * @return node ID
     */
    int getNodeId();

    /**
     * Check if the node is currently active
     * @return true if node is active, false otherwise
     */
    boolean isActive();

    /**
     * Start the node and initialize its services
     */
    void start();

    /**
     * Gracefully shutdown the node
     */
    void shutdown();

    /**
     * Print current status and state of the node
     */
    void printStatus();

    /**
     * Handle node failure
     */
    void fail();

    /**
     * Recover node from failure
     */
    void recover();
}
