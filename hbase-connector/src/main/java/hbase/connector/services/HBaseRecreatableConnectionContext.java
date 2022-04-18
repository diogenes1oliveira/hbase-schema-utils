package hbase.connector.services;

import hbase.base.interfaces.IOSupplier;
import hbase.connector.interfaces.HBaseConnectionContext;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.TimedReadWriteLock;
import org.apache.hadoop.hbase.client.Connection;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Context that synchronizes the recreation of a context, using a Read/Write lock to avoid reconnections while the
 * connection is in use
 */
public class HBaseRecreatableConnectionContext implements HBaseConnectionContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRecreatableConnectionContext.class);
    private final IOSupplier<Connection> connectionFactory;
    private final TimedReadWriteLock readWriteLock;
    private final Object lock = new Object();
    @SuppressWarnings("java:S3077")
    private volatile Connection connection = null;

    /**
     * @param connectionCreator creator of new connection objects
     * @param readWriteLock     lock to synchronize read and write access to the connection
     */
    public HBaseRecreatableConnectionContext(IOSupplier<Connection> connectionCreator, TimedReadWriteLock readWriteLock) {
        this.connectionFactory = connectionCreator;
        this.readWriteLock = readWriteLock;
    }

    /**
     * Enters a Read-locked connection context
     * <p>
     * The connection is created via {@link #refresh()} if it still doesn't exist
     *
     * @return a proxy for the connection that locks the connection for Reads
     * @throws IOException failed to (re)connect to HBase
     */
    @Override
    public HBaseConnectionProxy enter() throws IOException {
        if (connection == null) {
            synchronized (lock) {
                if (connection == null) {
                    LOGGER.debug("Creating connection, it is null now");
                    refresh();
                }
            }
        }

        readWriteLock.lockRead();

        LOGGER.debug("Entered connection context");
        return new HBaseConnectionProxy(connection) {
            @Override
            public void close() {
                readWriteLock.unlockRead();
                LOGGER.debug("Left connection context");
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
        LOGGER.debug("Refreshing connection");
        readWriteLock.lockWrite();
        try {
            if (connection != null) {
                LOGGER.debug("Closing old connection");
                connection.close();
                connection = null;
            }
            connection = connectionFactory.get();
            LOGGER.debug("Connection successfully refreshed");
        } finally {
            readWriteLock.unlockWrite();
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
        readWriteLock.lockWrite();
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } finally {
            readWriteLock.unlockWrite();
        }

    }


}
