package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int maxPages;
    private ConcurrentHashMap<PageId, Page> cachePage;
    LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxPages = numPages;
        this.cachePage = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public LockManager getLockManager() {
        return this.lockManager;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        boolean gotLock = false;
        while (!gotLock) {
            synchronized (this) {
                gotLock = (perm == Permissions.READ_ONLY) ?
                        lockManager.getShardedLock(pid, tid) :
                        lockManager.getExclusiveLock(pid, tid);
            }
            if (!gotLock) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        if (cachePage.containsKey(pid)) {
            return cachePage.get(pid);
        }

        if (cachePage.size() >= maxPages) {
            evictPage();
        }

        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);
        cachePage.put(pid, page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        this.lockManager.releaseExclusiveLock(pid, tid);
        this.lockManager.releaseShardLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.lockManager.hasExclusiveLock(p, tid) || this.lockManager.hasShardLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        for (Map.Entry<PageId, Page> entry : cachePage.entrySet()) {
            PageId pageId = entry.getKey();
            Page page = entry.getValue();

            // Check if the page has been modified by the current transaction.
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                if (commit) {
                    flushPage(pageId);
                } else {
                    Page newPage = Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
                    newPage.markDirty(false, null);
                    cachePage.put(pageId, newPage);
                }
            }
        }

        // Release all locks held by the transaction.
        cachePage.keySet().forEach(pageId -> {
            lockManager.releaseExclusiveLock(pageId, tid);
            lockManager.releaseShardLock(pageId, tid);
        });

        lockManager.removeFromGraph(tid);
    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modifiedPages = heapFile.insertTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            cachePage.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modifiedPages = heapFile.deleteTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            cachePage.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : this.cachePage.keySet()) {
            this.flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        this.cachePage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = cachePage.get(pid);
        if (page == null) {
            throw new IOException();
        }
        if (page.isDirty() != null) {
            HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            f.writePage(page);
            page.markDirty(false, null);
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // Check if the buffer pool has reached its maximum capacity.
        if (cachePage.keySet().size() == maxPages) {
            PageId pageIdEvict = null;
            for (Map.Entry<PageId, Page> entry : cachePage.entrySet()) {
                // If a page is not dirty, select it for eviction.
                if (entry.getValue().isDirty() == null) {
                    pageIdEvict = entry.getKey();
                    break;
                }
            }

            // If no non-dirty page is found, throw an exception.
            if (pageIdEvict == null) {
                throw new DbException("No pages to evict");
            }

            try {
                flushPage(pageIdEvict);
            } catch (IOException e) {
                throw new DbException("Cannot flush the page to disk during eviction");
            }

            cachePage.remove(pageIdEvict);
        }
    }


}
