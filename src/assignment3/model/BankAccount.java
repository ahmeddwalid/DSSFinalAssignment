package assignment3.model;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe bank account implementation
 * Uses read-write locks for concurrent access
 */
public class BankAccount {
    private final String accountId;
    private double balance;
    private final ReentrantReadWriteLock lock; // Provides separate locks for read and write operations

    /**
     * Constructor for BankAccount
     * @param accountId unique account identifier
     * @param initialBalance starting balance
     */
    public BankAccount(String accountId, double initialBalance) {
        this.accountId = accountId;
        this.balance = initialBalance;
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Get current balance (thread-safe read)
     * Acquires a read lock, allowing concurrent reads if no write lock is held.
     * @return current account balance
     */
    public double getBalance() {
        lock.readLock().lock();
        try {
            return balance;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Deposit money to account (thread-safe write)
     * Acquires a write lock, ensuring exclusive access for modification.
     * @param amount amount to deposit
     * @return true if successful (amount is positive)
     */
    public boolean deposit(double amount) {
        if (amount <= 0) {
            // Or throw IllegalArgumentException("Deposit amount must be positive");
            return false;
        }

        lock.writeLock().lock();
        try {
            balance += amount;
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Withdraw money from account (thread-safe write)
     * Acquires a write lock, ensuring exclusive access for modification.
     * @param amount amount to withdraw
     * @return true if successful (amount is positive and sufficient funds)
     */
    public boolean withdraw(double amount) {
        if (amount <= 0) {
            // Or throw IllegalArgumentException("Withdrawal amount must be positive");
            return false;
        }

        lock.writeLock().lock();
        try {
            if (balance >= amount) {
                balance -= amount;
                return true;
            }
            return false; // Insufficient funds
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Set balance directly (intended for reconciliation or initialization, use with caution)
     * Acquires a write lock.
     * @param newBalance new balance value
     */
    public void setBalance(double newBalance) {
        lock.writeLock().lock();
        try {
            this.balance = newBalance;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the account ID.
     * @return The unique identifier for this bank account.
     */
    public String getAccountId() {
        return accountId;
    }

    @Override
    public String toString() {
        // Uses getBalance() to ensure thread-safe access to balance for string representation
        return String.format("Account{id='%s', balance=%.2f}", accountId, getBalance());
    }
}
