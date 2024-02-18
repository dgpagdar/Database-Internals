package simpledb;

import java.util.*;

/**
 * LockManager class manages locks on the pages to prevent data inconsistency like dirty-reads
 * and all in concurrent transaction, and also prevents deadlocks.
 */
public class LockManager {

    // Holds exclusive lock on a page
    Map<PageId, TransactionId> exclusiveLocks;

    // Holds shard lock on a page
    Map<PageId, Set<TransactionId>> shardLocks;

    // Graph to hold the transc dependencies to identify deadlock
    Map<TransactionId, Set<TransactionId>> graph;

    public LockManager() {
        exclusiveLocks = new HashMap<>();
        shardLocks = new HashMap<>();
        graph = new HashMap<>();
    }

    /**
     * Tries to get a shard lock for a transaction on the given page.
     * If page is exclusively locked by other transaction, then
     * adds dependency to graph and lock fails
     *
     * @param pageId        Id of the page in which the lock will be acquired.
     * @param transactionId transactionId Id of transaction who is requesting the lock.
     * @return true if shard lock is acquired, false otherwise
     * @throws TransactionAbortedException If lock leads to a deadlock.
     */
    public synchronized boolean getShardedLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        if (exclusiveLocks.containsKey(pageId) && !transactionId.equals(exclusiveLocks.get(pageId))) {
            if (!addToGraph(exclusiveLocks.get(pageId), transactionId)) {
                throw new TransactionAbortedException();
            }
            return false;
        } else {
            shardLocks.computeIfAbsent(pageId, k -> new HashSet<>()).add(transactionId);
            return true;
        }
    }

    /**
     * Release the shard lock acquired by the transaction on the page
     *
     * @param pageId        Id of the page in which the lock will be released.
     * @param transactionId Id of transaction who is releasing the lock.
     * @return true if shard lock is released
     */
    public synchronized boolean releaseShardLock(PageId pageId, TransactionId transactionId) {
        if (shardLocks.containsKey(pageId)) {
            shardLocks.get(pageId).remove(transactionId);
        }
        return true;
    }

    /**
     * Tries to get a exclusive lock for a transaction on the given page.
     * Also checks for conflicts with existing locks.
     *
     * @param pageId        Id of the page in which the lock will be acquired.
     * @param transactionId transactionId Id of transaction who is requesting the lock.
     * @return true if shard lock is acquired, false otherwise
     * @throws TransactionAbortedException If lock leads to a deadlock.
     */
    public synchronized boolean getExclusiveLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        if (exclusiveLocks.containsKey(pageId) && !exclusiveLocks.get(pageId).equals(transactionId)) {
            if (!addToGraph(exclusiveLocks.get(pageId), transactionId))
                throw new TransactionAbortedException();
            return false;
        }

        if (shardLocks.containsKey(pageId) && !shardLocks.get(pageId).isEmpty()) {
            if (shardLocks.get(pageId).size() == 1 && shardLocks.get(pageId).contains(transactionId)) {
                if (detectDeadLock(transactionId, pageId)) throw new TransactionAbortedException();
                exclusiveLocks.put(pageId, transactionId);
                return true;
            }

            for (TransactionId tid : shardLocks.get(pageId)) {
                if (!addToGraph(tid, transactionId)) throw new TransactionAbortedException();
            }
            return false;
        }

        if (detectDeadLock(transactionId, pageId)) throw new TransactionAbortedException();
        exclusiveLocks.put(pageId, transactionId);
        return true;
    }

    /**
     * Release the exclusive lock acquired by the transaction on the page
     *
     * @param pageId        Id of the page in which the lock will be released.
     * @param transactionId Id of transaction who is releasing the lock.
     * @return true if shard lock is released
     */
    public synchronized boolean releaseExclusiveLock(PageId pageId, TransactionId transactionId) {
        return !exclusiveLocks.containsKey(pageId) || exclusiveLocks.get(pageId).equals(transactionId) && exclusiveLocks.remove(pageId) != null;
    }

    /**
     * Adds an dependency of a transaction to a graph for deadlock.
     *
     * @param tid1 transaction which is dependent on another.
     * @param tid2 transaction being waited on
     * @return true, if dependency is added without detecting deadlock.
     * false, if deadlock detected in adding dependency
     */
    public synchronized boolean addToGraph(TransactionId tid1, TransactionId tid2) {
        if (detectDeadLockExists(tid2, tid1)) {
            return false;
        }
        graph.computeIfAbsent(tid1, k -> new HashSet<>()).add(tid2);
        return true;
    }

    /**
     * Removes a transaction and dependencies from the graph
     *
     * @param transactionId Id of the transaction to remove
     */
    public synchronized void removeFromGraph(TransactionId transactionId) {
        graph.remove(transactionId);

        graph.values().forEach(set -> set.remove(transactionId));
    }

    /**
     * Check if deadlock occurs if transaction waits for the given page
     *
     * @param transactionId If of the transaction
     * @param pageId        Id of the page
     * @return true if deadlock detected, false otherwise
     */
    public synchronized boolean detectDeadLock(TransactionId transactionId, PageId pageId) {
        TransactionId transactionId1 = exclusiveLocks.get(pageId);
        if (transactionId1 != null && !transactionId1.equals(transactionId)) {
            if (graph.getOrDefault(transactionId, Collections.emptySet()).contains(transactionId1)) {
                return true;
            }
            graph.computeIfAbsent(transactionId1, k -> new HashSet<>()).add(transactionId);
        }
        return false;
    }

    /**
     * Checks if deadlock exists directly between two transactions
     *
     * @param tid1 Id of first transaction
     * @param tid2 Id of second transaction
     * @return true if deadlock exists, false otherwise
     */
    public synchronized boolean detectDeadLockExists(TransactionId tid1, TransactionId tid2) {
        return graph.containsKey(tid1) && graph.get(tid1).contains(tid2);
    }

    /**
     * Checks if a transaction holds a shared lock on a page.
     *
     * @param pageId ID of the page to check.
     * @param tid    ID of the transaction.
     * @return true if the transaction holds a shared lock, false otherwise.
     */
    public synchronized boolean hasShardLock(PageId pageId, TransactionId tid) {
        return shardLocks.containsKey(pageId) && shardLocks.get(pageId).contains(tid);
    }

    /**
     * Checks if a transaction holds a exclusive lock on a page.
     *
     * @param pageId ID of the page to check.
     * @param tid    ID of the transaction.
     * @return true if the transaction holds a shared lock, false otherwise.
     */
    public synchronized boolean hasExclusiveLock(PageId pageId, TransactionId tid) {
        return exclusiveLocks.get(pageId) != null && exclusiveLocks.get(pageId).equals(tid);
    }
}