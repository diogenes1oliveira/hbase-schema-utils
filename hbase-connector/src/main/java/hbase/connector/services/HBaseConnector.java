package hbase.connector.services;

import hadoop.kerberos.utils.interfaces.IOSupplier;
import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static hbase.connector.utils.HBaseHelpers.getMillisDuration;
import static hbase.connector.utils.HBaseHelpers.toHBaseConf;

/**
 * Manages a singleton instance of a HBase connection
 * <p>
 * {@link HBaseConnectionFactoryRegistry} is used to get the suitable factory for the given configuration
 * <p>
 * Obs: connection and disconnection are mutually synchronized
 */
public class HBaseConnector {
    private final HBaseRecreatableConnectionContext connectionContext;

    /**
     * Automatic reconnection period
     */
    public static final String CONFIG_RECONNECTION_PERIOD = "hbase.custom.reconnection.period";

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
        this.connectionContext = newContext(conf);
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
    public HBaseConnectionProxy context() throws IOException {
        return connectionContext.enter();
    }

    /**
     * Closes the current connection and creates it back up
     */
    public void reconnect() throws IOException {
        connectionContext.refresh();
    }

    /**
     * Closes the currently open connection
     * <p>
     * Obs: this method is idempotent, i.e., it's a no-op if already disconnected
     *
     * @throws IOException failed to close the currently open connection
     */
    public void disconnect() throws IOException {
        connectionContext.disconnect();
    }

    /**
     * Checks if there's an active connection
     */
    public boolean isConnected() {
        return connectionContext.getUnproxiedConnection() != null;
    }

    protected HBaseRecreatableConnectionContext newContext(Configuration conf) {
        long expireMillis = getMillisDuration(conf, CONFIG_RECONNECTION_PERIOD, 0L);
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        HBaseConnectionFactory connectionFactory = new HBaseConnectionFactoryRegistry();
        IOSupplier<Connection> connectionCreator = () -> connectionFactory.create(conf);

        if (expireMillis > 0) {
            return new HBaseExpirableConnectionContext(expireMillis, connectionCreator, readWriteLock);
        } else {
            return new HBaseRecreatableConnectionContext(connectionCreator, readWriteLock);
        }
    }
}
