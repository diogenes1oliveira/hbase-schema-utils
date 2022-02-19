package hbase.connector.utils;

import hbase.connector.interfaces.HBaseConnectionFactory;
import hbase.connector.interfaces.HBaseUncheckedConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.time.Duration;
import java.util.Optional;

public class HBaseRefreshingConnection extends HBaseUncheckedConnectionProxy {
    /**
     * Automatic reconnection period
     */
    public static final String CONFIG_RECONNECTION_PERIOD = "custom.hbase.reconnection.period";

    private volatile Connection connection;
    private final long refreshMillis;
    private final HBaseConnectionFactory factory;

    public HBaseRefreshingConnection(Connection initialConnection,
                                     HBaseConnectionFactory factory) {
        this.connection = initialConnection;
        this.factory = factory;

        this.refreshMillis = getRefreshPeriod(initialConnection.getConfiguration())
                .map(Duration::toMillis)
                .orElse(-1L);
    }

    @Override
    protected Connection getWrappedConnection() {
        return connection;
    }

    public static Optional<Duration> getRefreshPeriod(Configuration conf) {
        return Optional.empty();
    }
}
