package hbase.connector;

import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.ContextReadWriteLock;
import hbase.connector.proxies.HBaseExpirableConnection;
import hbase.connector.proxies.HBaseRecreatableConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static hbase.connector.utils.HBaseHelpers.toHBaseConf;

/**
 * Manages a singleton instance of a HBase connection
 * <p>
 * {@link HBaseRegistryConnectionFactory} is used to get the suitable factory for the given configuration
 * <p>
 * Obs: connection and disconnection are mutually synchronized
 */
public class HBaseConnector implements AutoCloseable {
    private final HBaseExpirableConnection connection;

    /**
     * @param props Java properties for the new connection
     */
    public HBaseConnector(Properties props) {
        this(toHBaseConf(props));
    }

    /**
     * @param propsMap map of properties for the new connection
     */
    public HBaseConnector(Map<String, String> propsMap) {
        this(toHBaseConf(propsMap));
    }

    /**
     * @param conf Hadoop-style configuration for the connection
     */
    public HBaseConnector(Configuration conf) {
        this.connection = newConnection(conf, new HBaseRegistryConnectionFactory(), new ContextReadWriteLock());
    }

    /**
     * Creates a new connection or returns the current one
     * <p>
     * You do need to use this in a try-with-resources fashion, because the returned connection's method
     * {@link Connection#close()} is overriden with locks to avoid concurrent reconnections
     * <p>
     *
     * @return HBase connection object
     * @throws IOException failed to create connection
     */
    public HBaseConnectionProxy connect() throws IOException {
        return connection.getWrappedConnection();
    }

    /**
     * Closes the current connection and creates it back up
     */
    public void reconnect() throws IOException {
        close();
        connection.refreshNow();
    }

    /**
     * Closes the currently open connection
     * <p>
     * Obs: this method is idempotent, i.e., it's a no-op if already disconnected
     *
     * @throws IOException failed to close the currently open connection
     */
    @Override
    public void close() throws IOException {
        connection.close();
    }

    /**
     * Checks if there's an active connection
     */
    public boolean isConnected() {
        return connection.getUnproxiedConnection() != null;
    }

    private static HBaseExpirableConnection newConnection(Configuration conf,
                                                          HBaseConnectionFactory factory,
                                                          ContextReadWriteLock contextLock) {
        HBaseRecreatableConnection connection = new HBaseRecreatableConnection(
                conf, factory, contextLock
        );
        return new HBaseExpirableConnection(connection);
    }
}
