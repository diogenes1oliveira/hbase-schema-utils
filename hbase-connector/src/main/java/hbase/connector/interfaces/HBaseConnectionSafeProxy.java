package hbase.connector.interfaces;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableBuilder;

import java.util.concurrent.ExecutorService;

/**
 * Extension to {@link HBaseConnectionProxy} that can't throw in its {@link #getWrappedConnection()} method
 */
public abstract class HBaseConnectionSafeProxy extends HBaseConnectionProxy {
    /**
     * Gets the current connection, potentially wrapped in multiple proxies
     */
    @Override
    protected abstract Connection getWrappedConnection();

    @Override
    public Configuration getConfiguration() {
        return getWrappedConnection().getConfiguration();
    }

    @Override
    public boolean isClosed() {
        return getWrappedConnection().isClosed();
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        return getWrappedConnection().getTableBuilder(tableName, executorService);
    }

    @Override
    public void abort(String s, Throwable throwable) {
        getWrappedConnection().abort(s, throwable);
    }

    @Override
    public boolean isAborted() {
        return getWrappedConnection().isAborted();
    }
}
