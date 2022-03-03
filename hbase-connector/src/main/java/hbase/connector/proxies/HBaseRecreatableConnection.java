package hbase.connector.proxies;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.interfaces.LockContext;
import hbase.connector.utils.ContextReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Wraps over a Connection to offer synchronized recreation
 */
public class HBaseRecreatableConnection extends HBaseConnectionProxy {
    private volatile Connection connection = null;
    private final HBaseConnectionFactory factory;
    private final ContextReadWriteLock contextLock;
    private final Configuration conf;

    /**
     * @param conf        Hadoop-style configuration for the new connection
     * @param factory     factory for the new refreshed HBase connections
     * @param contextLock lock to be acquired before reading or writing the connection
     */
    public HBaseRecreatableConnection(Configuration conf,
                                      HBaseConnectionFactory factory,
                                      ContextReadWriteLock contextLock) {
        this.conf = conf;
        this.factory = factory;
        this.contextLock = contextLock;
    }

    /**
     * Gets the underlying real connection object in a synchronized fashion
     * <p>
     * This will call {@link #recreate()} the first time to initialize the connection
     *
     * @return current connection
     * @throws IOException connection failed to be created the first time
     */
    @Override
    public HBaseConnectionProxy getWrappedConnection() throws IOException {
        LockContext context = contextLock.lockRead();
        if (connection == null) {
            try {
                recreate();
            } catch (IOException | RuntimeException e) {
                context.close();
                throw e;
            }
        }
        return new HBaseCloseCallbackConnection(connection, c -> context.close());
    }

    /**
     * Gets the underlying non-synchronized connection instance
     */
    @Nullable
    @Override
    public Connection getUnproxiedConnection() {
        return connection;
    }

    /**
     * Close the current connection and create it back up in a synchronized fashion
     *
     * @throws IOException connection failed to be closed or re-recreated
     */
    public void recreate() throws IOException {
        //noinspection unused
        try (LockContext context = contextLock.lockWrite()) {
            if (connection != null) {
                connection.close();
            }
            connection = factory.create(conf);
        }
    }

    /**
     * Overrides the close method to call {@link Connection#close()} in a synchronized and idempotent fashion
     */
    @Override
    public void close() throws IOException {
        try (LockContext context = contextLock.lockWrite()) {
            if (connection != null) {
                connection.close();
            }
            connection = null;
        }
    }

    // the methods below need to be implemented manually because getWrappedConnection() might throw, so
    // I can't use HBaseConnectionSafeProxy
    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public boolean isClosed() {
        return connection == null || connection.isClosed();
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        if (connection == null) {
            throw new IllegalStateException("Connection not initialized yet");
        }
        return connection.getTableBuilder(tableName, executorService);
    }

    @Override
    public void abort(String s, Throwable throwable) {
        if (connection != null) {
            connection.abort(s, throwable);
        }
    }

    @Override
    public boolean isAborted() {
        return connection != null && connection.isAborted();
    }

}
