package common.utils;

import java.time.LocalTime; // Changed from LocalDateTime
import java.time.format.DateTimeFormatter;

/**
 * Simple logging utility for the distributed system.
 * Implements Singleton pattern for consistent logging across all components.
 */
public class Logger {
    private static Logger instance;
    // DateTimeFormatter for timestamping log messages
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private boolean debugEnabled = false; // Debug logging can be toggled

    // Private constructor to prevent instantiation
    private Logger() {}

    /**
     * Get the singleton instance of the logger.
     * Ensures only one logger instance is used throughout the application.
     * @return The singleton Logger instance.
     */
    public static synchronized Logger getInstance() {
        if (instance == null) {
            instance = new Logger();
        }
        return instance;
    }

    /**
     * Logs an informational message.
     * @param message The message to log.
     */
    public void info(String message) {
        log("INFO", message);
    }

    /**
     * Logs a debug message. Only logs if debug mode is enabled.
     * @param message The message to log.
     */
    public void debug(String message) {
        if (debugEnabled) {
            log("DEBUG", message);
        }
    }

    /**
     * Logs an error message.
     * @param message The message to log.
     */
    public void error(String message) {
        log("ERROR", message);
    }

    /**
     * Logs a warning message.
     * @param message The message to log.
     */
    public void warn(String message) {
        log("WARN", message);
    }

    /**
     * Logs a system message (typically without specific node context in the message itself).
     * @param level The log level (e.g., "SYSTEM", "SETUP").
     * @param message The message to log.
     */
    public void system(String level, String message) {
        String timestamp = LocalTime.now().format(formatter);
        System.out.printf("[%s] [%s] %s%n", timestamp, level.toUpperCase(), message);
    }

    /**
     * Internal generic logging method.
     * Prepends a timestamp and log level to the message.
     * @param level The log level (e.g., "INFO", "DEBUG", "ERROR").
     * @param message The message content.
     */
    private void log(String level, String message) {
        String timestamp = LocalTime.now().format(formatter); // Using LocalTime for HH:mm:ss.SSS
        System.out.printf("[%s] [%s] %s%n", timestamp, level, message);
    }

    /**
     * Enables or disables debug logging.
     * @param enabled true to enable debug logs, false to disable.
     */
    public void setDebugEnabled(boolean enabled) {
        this.debugEnabled = enabled;
        if (enabled) {
            info("Debug logging enabled.");
        } else {
            info("Debug logging disabled.");
        }
    }

    /**
     * Prints a separator line to the console for better log readability.
     */
    public void separator() {
        System.out.println("======================================================================");
    }

    /**
     * Prints a section header to the console, useful for delineating phases of execution.
     * @param title The title of the section.
     */
    public void section(String title) {
        System.out.println();
        separator();
        System.out.println("=== " + title.toUpperCase() + " ===");
        separator();
        System.out.println();
    }
}
