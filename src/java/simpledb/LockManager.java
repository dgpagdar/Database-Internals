package simpledb;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

/**
 * LockManager class manages locks on the pages to prevent data inconsistency like dirty-reads
 * and all in concurrent transaction.
 */
public class LockManager {

    // Holds exclusive lock on a page
    Map<PageId, TransactionId> exclusiveLocks;

    // Holds shard lock on a page
    Map<PageId, Set<TransactionId>> shardLocks;

    /**
     * Initialize lock manager with the empty structure for managing locks
     */
    public LockManager() {
        exclusiveLocks = new HashMap<>();
        shardLocks = new HashMap<>();
    }

    /**
     * Tries to get a shard lock for a transaction on the given page.
     * It also allows multiple transactions to read the same page.
     *
     * @param pageId        Id of the page in which the lock will be acquired.
     * @param transactionId Id of transaction who is requesting the lock.
     * @return true if shard lock is acquired, false otherwise
     */
    public synchronized boolean getShardedLock(PageId pageId, TransactionId transactionId) {
        if (exclusiveLocks.containsKey(pageId)) {
            if (!transactionId.equals(exclusiveLocks.get(pageId))) {
                return false;
            }
        }
        shardLocks.computeIfAbsent(pageId, k -> new HashSet<>()).add(transactionId);
        return true;
    }

    /**
     * Release the shard lock acquired by the transaction on the page
     *
     * @param pageId        Id of the page in which the lock will be released.
     * @param transactionId Id of transaction who is releasing the lock.
     * @return true if shard lock is released
     */
    public synchronized boolean releaseSharededLock(PageId pageId, TransactionId transactionId) {
        Set<TransactionId> transc = shardLocks.get(pageId);
        if (transc != null) {
            transc.remove(transactionId);
        }
        return true;
    }

    /**
     * Tries to get a exclusive lock for a transaction on the given page.
     * It prevents other transaction from reading or writing to the page.
     *
     * @param pageId        Id of the page in which the lock will be acquired.
     * @param transactionId Id of transaction who is requesting the lock.
     * @return true if shard lock is acquired, false otherwise
     */
    public synchronized boolean getExclusiveLock(PageId pageId, TransactionId transactionId) {
        if (exclusiveLocks.containsKey(pageId)) {
            return false;
        }

        Set<TransactionId> transc = shardLocks.get(pageId);
        if (transc == null || (transc.size() == 1 && transc.contains(transactionId))) {
            exclusiveLocks.put(pageId, transactionId);
            return true;
        }

        return false;
    }

    /**
     * Release the exclusive lock acquired by the transaction on the page
     *
     * @param pageId        Id of the page in which the lock will be released.
     * @param transactionId Id of transaction who is releasing the lock.
     * @return true if shard lock is released
     */
    public synchronized boolean releaseExclusiveLock(PageId pageId, TransactionId transactionId) {
        if (!transactionId.equals(exclusiveLocks.get(pageId))) {
            return false;
        }
        exclusiveLocks.remove(pageId);
        return true;
    }

    /**
     * Checks if a transaction holds a shared lock on a page.
     *
     * @param pageId        ID of the page to check.
     * @param transactionId ID of the transaction.
     * @return true if the transaction holds a shared lock, false otherwise.
     */
    public synchronized boolean hasShardLock(PageId pageId, TransactionId transactionId) {
        Set<TransactionId> transc = shardLocks.get(pageId);
        return transc != null && transc.contains(transactionId);
    }

    /**
     * Checks if a transaction holds a exclusive lock on a page.
     *
     * @param pageId        ID of the page to check.
     * @param transactionId ID of the transaction.
     * @return true if the transaction holds a shared lock, false otherwise.
     */
    public synchronized boolean hasExclusiveLock(PageId pageId, TransactionId transactionId) {
        return transactionId.equals(exclusiveLocks.get(pageId));
    }
}