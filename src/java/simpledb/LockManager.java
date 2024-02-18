package simpledb;

import java.util.*;
import java.util.Map.Entry;

public class LockManager {

    Map<PageId, TransactionId> exclusiveLock;
    Map<PageId, Set<TransactionId>> readLock;
    Map<TransactionId, Set<TransactionId>> dependencyMapping;

    public LockManager() {
        exclusiveLock = new HashMap<>();
        readLock = new HashMap<>();
        dependencyMapping = new HashMap<>();
    }


    public synchronized boolean getReadLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        // Check if the page has an exclusive lock by another transaction
        if (exclusiveLock.containsKey(pageId) && !transactionId.equals(exclusiveLock.get(pageId))) {
            // If the current transaction does not hold the exclusive lock, manage dependency
            if (!addToDependencyMapping(exclusiveLock.get(pageId), transactionId)) {
                throw new TransactionAbortedException(); // Dependency failed, abort transaction
            }
            return false; // Cannot acquire read lock due to exclusive lock by another transaction
        } else {
            // No exclusive lock conflict or current transaction holds the exclusive lock, proceed with read lock
            readLock.computeIfAbsent(pageId, k -> new HashSet<>()).add(transactionId);
            return true; // Read lock acquired or updated
        }
    }


    public synchronized void removeFromDependency(TransactionId transactionId) {
        // Directly remove the transactionId as a key from the map
        dependencyMapping.remove(transactionId);

        // Iterate over the values of the map to remove the transactionId from each set
        dependencyMapping.values().forEach(set -> set.remove(transactionId));
    }


    public synchronized boolean releaseReadLock(PageId pageId, TransactionId transactionId) {
        if (readLock.containsKey(pageId)) {
            readLock.get(pageId).remove(transactionId);
        }
        return true;
    }


    public synchronized boolean identifyDeadLock(TransactionId transactionId, PageId pageId) {
        TransactionId lockHolder = exclusiveLock.get(pageId);
        if (lockHolder != null && !lockHolder.equals(transactionId)) {
            // Check if the current transaction is waiting on a transaction that holds an exclusive lock
            if (dependencyMapping.getOrDefault(transactionId, Collections.emptySet()).contains(lockHolder)) {
                return true; // Deadlock detected
            }
            // Update dependency mapping to reflect that the transaction holding the lock is now waiting on the current transaction
            dependencyMapping.computeIfAbsent(lockHolder, k -> new HashSet<>()).add(transactionId);
        }
        return false; // No deadlock detected
    }


    public synchronized boolean addToDependencyMapping(TransactionId toAddTo, TransactionId transactionId) {
        if (identifyDeadLockExists(transactionId, toAddTo)) {
            return false;
        }
        dependencyMapping.computeIfAbsent(toAddTo, k -> new HashSet<>()).add(transactionId);
        return true;
    }


    public synchronized boolean identifyDeadLockExists(TransactionId first, TransactionId second) {
        return dependencyMapping.containsKey(first) && dependencyMapping.get(first).contains(second);
    }


    public synchronized boolean getExclusiveLock(PageId pageId, TransactionId transactionId) throws TransactionAbortedException {
        // Check for existing exclusive lock by another transaction
        if (exclusiveLock.containsKey(pageId) && !exclusiveLock.get(pageId).equals(transactionId)) {
            if (!addToDependencyMapping(exclusiveLock.get(pageId), transactionId))
                throw new TransactionAbortedException();
            return false;
        }

        // Check for read locks
        if (readLock.containsKey(pageId) && !readLock.get(pageId).isEmpty()) {
            // If there's only one read lock by this transaction, allow exclusive lock
            if (readLock.get(pageId).size() == 1 && readLock.get(pageId).contains(transactionId)) {
                if (identifyDeadLock(transactionId, pageId)) throw new TransactionAbortedException();
                exclusiveLock.put(pageId, transactionId);
                return true;
            }

            // If multiple transactions hold the read lock, or a different transaction holds it, check dependencies
            for (TransactionId tid : readLock.get(pageId)) {
                if (!addToDependencyMapping(tid, transactionId)) throw new TransactionAbortedException();
            }
            return false;
        }

        // No read lock or exclusive lock conflicts, check for deadlocks then grant exclusive lock
        if (identifyDeadLock(transactionId, pageId)) throw new TransactionAbortedException();
        exclusiveLock.put(pageId, transactionId);
        return true;
    }


    public synchronized boolean releaseExclusiveLock(PageId pageId, TransactionId transactionId) {
        return !exclusiveLock.containsKey(pageId) || exclusiveLock.get(pageId).equals(transactionId) && exclusiveLock.remove(pageId) != null;
    }


    public synchronized boolean hasReadLock(PageId pageId, TransactionId tid) {
        return readLock.containsKey(pageId) && readLock.get(pageId).contains(tid);
    }

    public synchronized boolean hasExclusiveLock(PageId pageId, TransactionId transactionId) {
        return exclusiveLock.get(pageId) != null && exclusiveLock.get(pageId).equals(transactionId);
    }

}