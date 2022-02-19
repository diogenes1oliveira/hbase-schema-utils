package hbase.connector.interfaces;

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
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;

/**
 * Generic Proxy (as in the design pattern) for a HBase connection
 * <p>
 * All {@link IOException} are wrapped in {@link UncheckedIOException}
 */
public abstract class HBaseUncheckedConnectionProxy implements Connection {
    /**
     * Returns the original wrapped connection
     */
    protected abstract Connection getWrappedConnection();

    @Override
    public Configuration getConfiguration() {
        return getWrappedConnection().getConfiguration();
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) {
        try {
            return getWrappedConnection().getBufferedMutator(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) {
        try {
            return getWrappedConnection().getBufferedMutator(bufferedMutatorParams);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) {
        try {
            return getWrappedConnection().getRegionLocator(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Admin getAdmin() {
        try {
            return getWrappedConnection().getAdmin();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            getWrappedConnection().close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    @Override
    public Table getTable(TableName tableName) {
        try {
            return getWrappedConnection().getTable(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) {
        try {
            return getWrappedConnection().getTable(tableName, pool);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
