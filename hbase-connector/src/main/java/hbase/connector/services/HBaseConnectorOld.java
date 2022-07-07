package hbase.connector.services;

import hadoop.kerberos.utils.UgiContextManager;
import hbase.base.interfaces.Config;
import hbase.base.interfaces.Configurable;
import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.TimedReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

import static hbase.connector.utils.HBaseHelpers.toHBaseConf;

/**
 * Manages an instance of a HBase connection
 * <p>
 * Obs: connection and disconnection are mutually synchronized
 */
public class HBaseConnectorOld implements Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnectorOld.class);


    private long expireMillis = 0L;
    private long readTimeoutMs = 60_000L;
    private long writeTimeoutMs = 60_000L * 5;
    private HBaseRecreatableConnectionContext connectionContext = null;

    /**
     * @param config config object
     */
    @Override
    public void configure(Config config) {
        expireMillis = config.getValue(HBaseConnectorConfig.RECONNECTION_PERIOD, expireMillis, Long.class);
        readTimeoutMs = config.getValue(HBaseConnectorConfig.LOCK_READ_TIMEOUT, readTimeoutMs, Long.class);
        writeTimeoutMs = config.getValue(HBaseConnectorConfig.LOCK_WRITE_TIMEOUT, writeTimeoutMs, Long.class);


        Configuration conf = toHBaseConf(config.getValue(HBaseConnectorConfig.PREFIX, new Properties(), Properties.class));
        String hadoopAuth = conf.getTrimmed(UgiContextManager.HADOOP_AUTH, "").toLowerCase(Locale.ROOT);

        if ("kerberos".equals(hadoopAuth)) {
            UgiContextManager.enableKerberos();
        }
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
        assureConfigured();
        LOGGER.debug("Will now enter the connection context");
        return connectionContext.enter();
    }

    /**
     * Closes the current connection and creates it back up
     */
    public void reconnect() throws IOException {
        assureConfigured();
        connectionContext.refresh();
    }

    /**
     * Closes the currently open connection
     * <p>
     * <li>this method is idempotent, i.e., it's a no-op if already disconnected;</li>
     * <li>you'll need to call {@link #configure(Config)} again after this call</li>
     *
     * @throws IOException failed to close the currently open connection
     */
    public void disconnect() throws IOException {
        if (connectionContext != null) {
            connectionContext.disconnect();
        }
    }

    /**
     * Checks if there's an active connection
     */
    public boolean isConnected() {
        return connectionContext != null && connectionContext.getUnproxiedConnection() != null;
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

    private void assureConfigured() {
        if (connectionContext == null) {
            throw new IllegalStateException("Connector not configured yet");
        }
    }
}
