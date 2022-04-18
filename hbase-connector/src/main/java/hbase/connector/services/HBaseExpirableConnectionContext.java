package hbase.connector.services;

import hbase.base.interfaces.IOSupplier;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.TimedReadWriteLock;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Context that auto-recreates the connection after an expire duration.
 * <p>
 * Because it inherits from {@link HBaseRecreatableConnectionContext}, it also synchronizes the recreation of a context using a Read/Write
 * lock to avoid reconnections while the connection is in use
 */
public class HBaseExpirableConnectionContext extends HBaseRecreatableConnectionContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseExpirableConnectionContext.class);
    private final long expireMillis;
    private final Object lock = new Object();
    private volatile long creationNanos = -1L;

    /**
     * @param expireMillis      automatic reconnection period in milliseconds
     * @param connectionCreator creator of new connection objects
     * @param readWriteLock     lock to synchronize read and write access to the connection
     */
    public HBaseExpirableConnectionContext(long expireMillis,
                                           IOSupplier<Connection> connectionCreator,
                                           TimedReadWriteLock readWriteLock) {
        super(connectionCreator, readWriteLock);
        this.expireMillis = expireMillis;
    }

    /**
     * Enters a Read-locked connection context
     * <p>
     * The connection is created via {@link HBaseRecreatableConnectionContext#refresh()} if it still doesn't exist
     * or is stale
     *
     * @return a proxy for the connection that locks the connection for Reads
     * @throws IOException failed to (re)connect to HBase
     */
    @Override
    public HBaseConnectionProxy enter() throws IOException {
        if (isExpired()) {
            synchronized (lock) {
                if (isExpired()) {
                    LOGGER.debug("Connection is expired, recreating now");
                    refresh();
                }
            }
        }
        return super.enter();
    }

    /**
     * Duration in milliseconds since the last connection recreation
     */
    public long getAgeMillis() {
        return (long) ((System.nanoTime() - creationNanos) / 1e6);
    }

    /**
     * true if the connection has been created, but its age has expired
     */
    public boolean isExpired() {
        return creationNanos >= 0 && getAgeMillis() > expireMillis;
    }

    /**
     * Recreate the connection right away
     */
    @Override
    public void refresh() throws IOException {
        super.refresh();
        creationNanos = System.nanoTime();
    }
}
