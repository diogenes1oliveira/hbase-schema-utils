package hbase.connector;

import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseUncheckedConnectionProxy;
import hbase.connector.utils.HBaseUncloseableConnection;
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
public class HBaseConnector {
    /**
     * Kerberos principal name
     */
    public static final String CONFIG_PRINCIPAL = "hbase.client.keytab.principal";
    /**
     * Kerberos keytab file
     */
    public static final String CONFIG_KEYTAB = "hbase.client.keytab.file";
    /**
     * Automatic reconnection period
     */
    public static final String CONFIG_RECONNECTION_PERIOD = "custom.hbase.reconnection.period";

    private volatile HBaseUncloseableConnection uncloseableConnection = null;
    private final Configuration conf;
    private final HBaseConnectionFactory factory = new HBaseRegistryConnectionFactory();
    private final Object lock = new Object();

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
     * @param conf Hadoop-style configuration for the new connection
     */
    public HBaseConnector(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Creates a new connection or returns the current one
     * <p>
     * You don't need to use this in a try-with-resources fashion, because the returned connection's method
     * {@link Connection#close()} is overriden as no-op. Call {@link #disconnect()} to actually close the current
     * connection
     * <p>
     * Obs: this method is idempotent, i.e., it's a no-op if already connected
     *
     * @return HBase connection object
     * @throws IOException failed to create connection
     */
    public HBaseUncheckedConnectionProxy connect() throws IOException {
        synchronized (lock) {
            if (uncloseableConnection != null) {
                return uncloseableConnection;
            }
            uncloseableConnection = new HBaseUncloseableConnection(factory.create(conf));
            return uncloseableConnection;
        }
    }

    /**
     * Closes the currently open connection
     * <p>
     * Obs: this method is idempotent, i.e., it's a no-op if already disconnected
     *
     * @throws IOException failed to close the currently open connection
     */
    public void disconnect() throws IOException {
        synchronized (lock) {
            if (uncloseableConnection == null) {
                return;
            }
            uncloseableConnection.getWrappedConnection().close();
            uncloseableConnection = null;
        }
    }

    /**
     * Checks if there's an active connection
     */
    boolean isConnected() {
        return uncloseableConnection != null;
    }

    /**
     * Applies
     *
     * The default implementation
     * @param original
     * @return
     */
    protected Connection applyWrappers(Connection original) {
        return original;
    }
}
