package hbase.connector.utils;

import hbase.connector.interfaces.HBaseUncheckedConnectionProxy;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Wraps over a Connection to disable its {@link Connection#close()} method
 */
public class HBaseUncloseableConnection extends HBaseUncheckedConnectionProxy {
    private final Connection connection;

    /**
     * @param connection wrapped connection object
     */
    public HBaseUncloseableConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * Overrides {@link Connection#close()} to be a no-op
     */
    @Override
    public void close() {
        // do nothing
    }

    /**
     * Returns the original wrapped connection
     */
    @Override
    public Connection getWrappedConnection() {
        return connection;
    }
}
