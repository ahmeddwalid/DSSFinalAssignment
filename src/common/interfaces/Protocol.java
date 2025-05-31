package common.interfaces;

/**
 * Base interface for distributed system protocols
 * Defines common methods for protocol implementation
 */
public interface Protocol {

    /**
     * Initialize the protocol
     */
    void initialize();

    /**
     * Start the protocol execution
     */
    void start();

    /**
     * Stop the protocol execution
     */
    void stop();

    /**
     * Get the protocol name
     * @return protocol identifier
     */
    String getProtocolName();

    /**
     * Check if protocol is currently running
     * @return true if running, false otherwise
     */
    boolean isRunning();
}
