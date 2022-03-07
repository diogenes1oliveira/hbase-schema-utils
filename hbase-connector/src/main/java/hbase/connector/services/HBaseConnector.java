package hbase.connector.services;

import hbase.base.interfaces.Config;
import hbase.base.interfaces.Configurable;
import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.TimedReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Properties;

import static hbase.connector.utils.HBaseHelpers.toHBaseConf;

/**
 * Manages an instance of a HBase connection
 * <p>
 * Obs: connection and disconnection are mutually synchronized
 */
public class HBaseConnector implements Configurable {
    private long expireMillis = 0L;
    private long readTimeoutMs = 60_000L;
    private long writeTimeoutMs = 60_000L * 5;
    private HBaseRecreatableConnectionContext connectionContext = null;

    /**
     * @param config config object
     */
    public void configure(Config config) {
        expireMillis = config.get(HBaseConnectorConfig.RECONNECTION_PERIOD);
        readTimeoutMs = config.get(HBaseConnectorConfig.LOCK_READ_TIMEOUT);
        writeTimeoutMs = config.get(HBaseConnectorConfig.LOCK_WRITE_TIMEOUT);

        Properties props = config.get(HBaseConnectorConfig.PREFIX);
        connectionContext = newContext(toHBaseConf(props));
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
        TimedReadWriteLock readWriteLock = new TimedReadWriteLock(readTimeoutMs, writeTimeoutMs);
        HBaseConnectionFactory factory = HBaseConnectionFactory.get(conf);

        if (expireMillis > 0) {
            return new HBaseExpirableConnectionContext(expireMillis, () -> factory.create(conf), readWriteLock);
        } else {
            return new HBaseRecreatableConnectionContext(() -> factory.create(conf), readWriteLock);
        }
    }

}
