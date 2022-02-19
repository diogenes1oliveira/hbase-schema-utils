package hbase.connector.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Generic Proxy (as in the design pattern) for a HBase connection
 */
public abstract class HBaseConnectionProxy implements Connection {
    /**
     * Gets the current connection, potentially wrapped in multiple proxies
     *
     * @throws IOException failed to get the current connection object
     */
    protected abstract Connection getWrappedConnection() throws IOException;

    /**
     * Gets the original Connection object
     * <p>
     * The default implementation just forwards to {@link #getWrappedConnection()}, so you do
     * need to reimplement this in case you're wrapping the connection in another proxy
     */
    @Nullable
    public Connection getUnproxiedConnection() throws IOException {
        return getWrappedConnection();
    }

    @Override
    public abstract Configuration getConfiguration();

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return getWrappedConnection().getBufferedMutator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
        return getWrappedConnection().getBufferedMutator(bufferedMutatorParams);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return getWrappedConnection().getRegionLocator(tableName);
    }

    @Override
    public Admin getAdmin() throws IOException {
        return getWrappedConnection().getAdmin();
    }

    @Override
    public void close() throws IOException {
        getWrappedConnection().close();
    }

    @Override
    public abstract boolean isClosed();

    @Override
    public abstract TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService);

    @Override
    public abstract void abort(String s, Throwable throwable);

    @Override
    public abstract boolean isAborted();

    @Override
    public Table getTable(TableName tableName) throws IOException {
        return getWrappedConnection().getTable(tableName);
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        return getWrappedConnection().getTable(tableName, pool);
    }
}
