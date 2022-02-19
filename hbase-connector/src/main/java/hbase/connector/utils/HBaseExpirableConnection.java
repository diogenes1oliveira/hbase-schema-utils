package hbase.connector.utils;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.connector.interfaces.HBaseConnectionProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static hbase.connector.utils.HBaseHelpers.getMillisDuration;

/**
 * Wraps over a {@link HBaseRecreatableConnection} to automatically recreate expired connections
 */
public class HBaseExpirableConnection extends HBaseConnectionProxy {

    /**
     * Automatic reconnection period
     */
    public static final String CONFIG_RECONNECTION_PERIOD = "custom.hbase.reconnection.period";

    /**
     * Max allowed age for a connection
     */
    public static final String CONFIG_EXPIRE_TIMEOUT = "custom.hbase.expiration.timeout";

    /**
     * Multiplier to derive the default expiration from the refresh period
     */
    public static final double DEFAULT_EXPIRATION_MULTIPLIER = 1.5;

    private final HBaseRecreatableConnection recreatableConnection;
    private final long refreshMillis;
    private final long expireMillis;
    private volatile long creationNanos;

    public HBaseExpirableConnection(HBaseRecreatableConnection recreatableConnection) {
        this.recreatableConnection = recreatableConnection;
        this.refreshMillis = getMillisDuration(recreatableConnection.getConfiguration(), CONFIG_RECONNECTION_PERIOD, -1L);
        this.expireMillis = getExpireMillis(recreatableConnection.getConfiguration(), refreshMillis);
        this.creationNanos = System.nanoTime();
    }

    /**
     * Gets the currently wrapped connection, calling {@link #refreshNow()} if it's expired
     *
     * @return wrapped connection
     * @throws IOException failed to create or get a query
     */
    @Override
    public HBaseConnectionProxy getWrappedConnection() throws IOException {
        if (isRefreshNeeded()) {
            refreshNow();
        }
        return recreatableConnection.getWrappedConnection();
    }

    /**
     * Gets the underlying non-synchronized connection
     */
    @Nullable
    @Override
    public Connection getUnproxiedConnection() {
        return recreatableConnection.getUnproxiedConnection();
    }

    /**
     * Immediately execute a recreation of the connection
     *
     * @throws IOException connection failed to be closed or re-recreated
     */
    public void refreshNow() throws IOException {
        recreatableConnection.recreate();
        creationNanos = System.nanoTime();
    }

    /**
     * Gets the number of mili-seconds since the last connection refresh (or creation)
     */
    public long getAgeMillis() {
        long deltaNanos = System.nanoTime() - creationNanos;
        return deltaNanos / 1_000_000;
    }

    /**
     * Checks if the connection needs to be refreshed
     */
    public boolean isRefreshNeeded() {
        return getAgeMillis() > refreshMillis;
    }

    /**
     * Checks if the expiration deadline is over already
     */
    public boolean isExpired() {
        return getAgeMillis() > expireMillis;
    }

    @Override
    public Configuration getConfiguration() {
        return recreatableConnection.getConfiguration();
    }

    @Override
    public boolean isClosed() {
        if (isExpired()) {
            throw expirationError();
        }
        return recreatableConnection.isClosed();
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        if (isExpired()) {
            throw expirationError();
        }
        return recreatableConnection.getTableBuilder(tableName, executorService);
    }

    @Override
    public void abort(String s, Throwable throwable) {
        if (isExpired()) {
            throw expirationError();
        }
        recreatableConnection.abort(s, throwable);
    }

    @Override
    public boolean isAborted() {
        if (isExpired()) {
            throw expirationError();
        }
        return recreatableConnection.isAborted();
    }

    private static long getExpireMillis(Configuration conf, long refreshMillis) {
        long defaultExpireMillis = (long) (refreshMillis * DEFAULT_EXPIRATION_MULTIPLIER);
        long expireMillis = getMillisDuration(conf, CONFIG_EXPIRE_TIMEOUT, defaultExpireMillis);

        if (expireMillis < refreshMillis) {
            throw new IllegalArgumentException(
                    "Expiration timeout can't be less than refresh period"
            );
        }

        return expireMillis;
    }

    private RuntimeException expirationError() {
        throw new IllegalStateException("Connection is expired");
    }
}
