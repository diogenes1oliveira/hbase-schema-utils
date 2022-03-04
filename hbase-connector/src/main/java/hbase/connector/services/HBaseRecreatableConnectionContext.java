package hbase.connector.services;

import hadoop.kerberos.utils.interfaces.IOSupplier;
import hbase.connector.interfaces.HBaseConnectionContext;
import hbase.connector.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.hbase.client.Connection;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Context that synchronizes the recreation of a context, using a Read/Write lock to avoid reconnections while the
 * connection is in use
 */
public class HBaseRecreatableConnectionContext implements HBaseConnectionContext {
    private final IOSupplier<Connection> connectionFactory;
    private final ReadWriteLock readWriteLock;
    private volatile Connection connection = null;
    private final Object lock = new Object();

    /**
     * @param connectionCreator creator of new connection objects
     * @param readWriteLock     lock to synchronize read and write access to the connection
     */
    public HBaseRecreatableConnectionContext(IOSupplier<Connection> connectionCreator, ReadWriteLock readWriteLock) {
        this.connectionFactory = connectionCreator;
        this.readWriteLock = readWriteLock;
    }

    /**
     * Enters a Read-locked connection context
     * <p>
     * The connection is created via {@link #refresh();} if it still doesn't exist
     *
     * @return a proxy for the connection that locks the connection for Reads
     * @throws IOException failed to (re)connect to HBase
     */
    @Override
    public HBaseConnectionProxy enter() throws IOException {
        if (connection == null) {
            synchronized (lock) {
                if (connection == null) {
                    refresh();
                }
            }
        }

        readWriteLock.readLock().lock();

        return new HBaseConnectionProxy(connection) {
            @Override
            public void close() {
                readWriteLock.readLock().unlock();
            }
        };
    }

    /**
     * Recreates the connection to HBase, destroying the current one if it's not null
     * <p>
     * The Write lock is acquired around the operation
     *
     * @throws IOException failed to close or create the connection to HBase
     */
    @Override
    public void refresh() throws IOException {
        readWriteLock.writeLock().lock();
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
            connection = connectionFactory.get();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Gets the original HBase connection object, might be null if not initialized yet
     */
    @Override
    public @Nullable Connection getUnproxiedConnection() {
        return connection;
    }

    /**
     * Closes the current connection if active
     *
     * @throws IOException failed to close
     */
    @Override
    public void disconnect() throws IOException {
        readWriteLock.writeLock().lock();
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }


}
