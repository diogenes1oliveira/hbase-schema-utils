package hbase.connector.utils;

import hbase.base.exceptions.UncheckedInterruptionException;
import hbase.base.exceptions.UncheckedTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wraps over a {@link ReentrantReadWriteLock} to provide checks for interruption and timeouts
 */
public class TimedReadWriteLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimedReadWriteLock.class);

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final long readTimeoutMs;
    private final long writeTimeoutMs;

    /**
     * @param readTimeoutMs  time to wait for the read lock
     * @param writeTimeoutMs time to wait for the write lock
     */
    public TimedReadWriteLock(long readTimeoutMs, long writeTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
        this.writeTimeoutMs = writeTimeoutMs;
    }

    /**
     * Locks for reads
     *
     * @throws UncheckedInterruptionException thread interrupted while waiting for lock
     * @throws UncheckedTimeoutException      read timeout exceeded
     */
    public void lockRead() {
        try {
            if (!readWriteLock.readLock().tryLock(readTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted while acquiring read lock", e);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            throw new UncheckedTimeoutException("read lock timeout exceeded", e);
        }
    }

    public boolean tryLockRead() {
        return readWriteLock.readLock().tryLock();
    }

    /**
     * Releases the read lock
     */
    public void unlockRead() {
        readWriteLock.readLock().unlock();
    }

    /**
     * Locks for writes
     *
     * @throws UncheckedInterruptionException thread interrupted while waiting for lock
     * @throws UncheckedTimeoutException      write timeout exceeded
     */
    public void lockWrite() {
        try {
            if (!readWriteLock.writeLock().tryLock(writeTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted while acquiring write lock", e);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            throw new UncheckedTimeoutException("write lock timeout exceeded", e);
        }
    }

    public boolean tryLockWrite() {
        return readWriteLock.writeLock().tryLock();
    }

    /**
     * Releases the write lock
     */
    public void unlockWrite() {
        readWriteLock.writeLock().unlock();
    }

}
