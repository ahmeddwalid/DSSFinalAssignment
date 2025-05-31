package assignment3.node;

import assignment3.model.BankAccount;
import assignment3.model.GossipMessage;
import assignment3.model.Transaction;
import assignment3.protocol.GossipProtocol; // Assuming this will be created
import common.interfaces.Node;
import common.utils.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a Bank Node in the Gossip-based banking system.
 * Each BankNode maintains local account balances and uses GossipProtocol
 * to propagate transactions and achieve eventual consistency.
 */
public class BankNode implements Node {
    private final int nodeId;
    private final Logger logger = Logger.getInstance();
    private final AtomicInteger lamportClock = new AtomicInteger(0); // Lamport clock for this node

    // Local state
    private final Map<String, BankAccount> accountBalances = new ConcurrentHashMap<>();
    private final Set<String> processedTransactionIds = ConcurrentHashMap.newKeySet(); // To avoid reprocessing

    // Network and Protocol
    private final List<BankNode> allPeers = new CopyOnWriteArrayList<>(); // List of all other nodes in the system
    private GossipProtocol gossipProtocol; // The gossip protocol handler for this node
    private final ExecutorService executorService = Executors.newCachedThreadPool(); // For async operations

    private volatile boolean isActive = false;
    private final ReentrantLock stateLock = new ReentrantLock(); // For critical sections involving local state

    /**
     * Constructor for BankNode.
     * @param nodeId Unique identifier for this node.
     */
    public BankNode(int nodeId) {
        this.nodeId = nodeId;
        // Initialize some default accounts for demonstration
        initializeDefaultAccounts();
    }

    private void initializeDefaultAccounts() {
        // Example: Node 1 creates ACC001, Node 2 creates ACC002, etc.
        // Or a shared set of accounts known to all nodes initially.
        // For simplicity, let's assume a few common accounts.
        accountBalances.put("ACC001", new BankAccount("ACC001", 1000.0));
        accountBalances.put("ACC002", new BankAccount("ACC002", 1500.0));
    }

    public void setPeers(List<BankNode> peers) {
        this.allPeers.clear();
        for (BankNode peer : peers) {
            if (peer.getNodeId() != this.nodeId) {
                this.allPeers.add(peer);
            }
        }
        // Initialize gossip protocol after peers are set
        this.gossipProtocol = new GossipProtocol(this, this.allPeers, executorService);
    }


    @Override
    public int getNodeId() {
        return nodeId;
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    @Override
    public void start() {
        isActive = true;
        logger.info(String.format("BankNode %d: Started. Lamport Clock: %d", nodeId, lamportClock.get()));
        if (gossipProtocol != null) {
            gossipProtocol.start(); // Start any periodic gossip tasks if necessary
        }
    }

    @Override
    public void shutdown() {
        isActive = false;
        logger.info(String.format("BankNode %d: Shutting down...", nodeId));
        if (gossipProtocol != null) {
            gossipProtocol.stop();
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info(String.format("BankNode %d: Shutdown complete.", nodeId));
    }

    /**
     * Creates a new banking transaction and initiates its propagation via gossip.
     * @param type The type of transaction (e.g., DEPOSIT, WITHDRAWAL).
     * @param amount The amount for the transaction.
     * @param accountId The account ID involved in the transaction.
     */
    public void createTransaction(Transaction.Type type, double amount, String accountId) {
        if (!isActive) {
            logger.warn(String.format("BankNode %d: Cannot create transaction, node is not active.", nodeId));
            return;
        }
        stateLock.lock();
        try {
            int currentLamportTime = lamportClock.incrementAndGet();
            Transaction transaction = new Transaction(type, amount, accountId, currentLamportTime, nodeId);
            logger.info(String.format("BankNode %d: Creating transaction: %s", nodeId, transaction));

            // Process locally first (important for causality if this node is the source)
            // and then gossip.
            if (processTransaction(transaction)) { // processTransaction also adds to processedTransactionIds
                if (gossipProtocol != null) {
                    gossipProtocol.initiateGossip(transaction);
                } else {
                    logger.warn(String.format("BankNode %d: Gossip protocol not initialized. Cannot gossip transaction %s", nodeId, transaction.getId()));
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Receives a gossiped transaction from another node.
     * @param gossipMessage The gossip message containing the transaction.
     */
    public void receiveGossip(GossipMessage gossipMessage) {
        if (!isActive) {
            logger.debug(String.format("BankNode %d: Not active, ignoring gossip: %s", nodeId, gossipMessage.getTransaction().getId()));
            return;
        }

        Transaction transaction = gossipMessage.getTransaction();
        stateLock.lock();
        try {
            // Update Lamport clock based on the transaction's timestamp
            lamportClock.set(Math.max(lamportClock.get(), transaction.getLamportTimestamp()) + 1);
            logger.debug(String.format("BankNode %d: Received gossip for %s. Lamport Clock updated to %d.",
                    nodeId, transaction.getId(), lamportClock.get()));

            if (processedTransactionIds.contains(transaction.getId())) {
                logger.debug(String.format("BankNode %d: Already processed transaction %s. Ignoring.", nodeId, transaction.getId()));
                return;
            }

            // Staleness check (As per assignment: "If an update is older than your state don't keep propagating it")
            // This interpretation: if the transaction's timestamp is significantly older than this node's current state for that account (hard to define without version vectors)
            // Or, simpler: if the transaction's timestamp is much older than the node's current lamport clock.
            if (transaction.isStale(lamportClock.get())) {
                logger.info(String.format("BankNode %d: Transaction %s is stale (ts: %d, local_ts: %d). Not processing or propagating.",
                        nodeId, transaction.getId(), transaction.getLamportTimestamp(), lamportClock.get()));
                return;
            }


            if (processTransaction(transaction)) {
                // If successfully processed, continue gossiping
                if (gossipProtocol != null) {
                    gossipProtocol.continueGossip(gossipMessage);
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Processes a transaction locally: applies changes to account balances.
     * @param transaction The transaction to process.
     * @return true if processed successfully, false otherwise (e.g., insufficient funds).
     */
    private boolean processTransaction(Transaction transaction) {
        // This method assumes lock is already held by caller if state modification occurs
        if (processedTransactionIds.contains(transaction.getId())) {
            return true; // Already processed, consider it successful for idempotency
        }

        BankAccount account = accountBalances.computeIfAbsent(
                transaction.getAccount(),
                accId -> new BankAccount(accId, 0.0) // Create account if it doesn't exist
        );
        boolean success = false;

        logger.info(String.format("BankNode %d: Processing %s on Account %s (Current Balance: %.2f)",
                nodeId, transaction.toString(), transaction.getAccount(), account.getBalance()));

        switch (Transaction.Type.valueOf(transaction.getType())) {
            case DEPOSIT:
                success = account.deposit(transaction.getAmount());
                break;
            case WITHDRAWAL:
                success = account.withdraw(transaction.getAmount());
                break;
            case TRANSFER:
                // Transfer logic would involve two accounts and potentially two transactions
                // For simplicity, this example might only handle one side of a transfer
                // or assume 'amount' is positive for deposit-like part, negative for withdrawal-like.
                logger.warn(String.format("BankNode %d: TRANSFER type not fully implemented in this basic example.", nodeId));
                success = false; // Mark as not successful for now
                break;
        }

        if (success) {
            processedTransactionIds.add(transaction.getId());
            logger.info(String.format("BankNode %d: Successfully processed %s. New Balance for %s: %.2f. Lamport time: %d",
                    nodeId, transaction.getId(), transaction.getAccount(), account.getBalance(), lamportClock.get()));
        } else {
            logger.warn(String.format("BankNode %d: Failed to process %s (e.g. insufficient funds for withdrawal). Account %s Balance: %.2f",
                    nodeId, transaction.getId(), transaction.getAccount(), account.getBalance()));
        }
        return success;
    }

    public int getCurrentLamportTime() {
        return lamportClock.get();
    }

    @Override
    public void printStatus() {
        stateLock.lock();
        try {
            logger.info(String.format(
                    "BankNode %d Status: [Active: %s, Lamport Clock: %d, Processed Transactions: %d]",
                    nodeId, isActive, lamportClock.get(), processedTransactionIds.size()));
            accountBalances.forEach((accId, account) -> {
                logger.info(String.format("  Account %s: Balance = %.2f", accId, account.getBalance()));
            });
        } finally {
            stateLock.unlock();
        }
    }

    // Methods required by Node interface but less relevant for this specific assignment part
    @Override
    public void fail() {
        this.isActive = false;
        logger.warn(String.format("BankNode %d: SIMULATING FAILURE.", nodeId));
    }

    @Override
    public void recover() {
        this.isActive = true;
        logger.info(String.format("BankNode %d: RECOVERED.", nodeId));
        // May need to re-sync or catch up with network state if applicable
    }
}
