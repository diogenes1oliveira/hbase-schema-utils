package hbase.connector.utils;

import hbase.connector.interfaces.LockContext;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wraps over a {@link ReentrantReadWriteLock} to offer an interface compatible with
 * try-with-resources
 */
public class ContextReadWriteLock {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Acquires the read lock and returns a context that unlocks it on close
     */
    public LockContext lockRead() {
        readWriteLock.readLock().lock();
        return () -> readWriteLock.readLock().unlock();
    }

    /**
     * Acquires the write lock and returns a context that unlocks it on close
     */
    public LockContext lockWrite() {
        readWriteLock.writeLock().lock();
        return () -> readWriteLock.writeLock().unlock();
    }

}
