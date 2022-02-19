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
import java.util.concurrent.ExecutorService;

/**
 * Generic Proxy (as in the design pattern) for a HBase connection
 */
public abstract class HBaseConnectionProxy implements Connection {
    protected abstract Connection getConnection();

    @Override
    public Configuration getConfiguration() {
        return getConnection().getConfiguration();
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return getConnection().getBufferedMutator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
        return getConnection().getBufferedMutator(bufferedMutatorParams);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return getConnection().getRegionLocator(tableName);
    }

    @Override
    public Admin getAdmin() throws IOException {
        return getConnection().getAdmin();
    }

    @Override
    public void close() throws IOException {
        getConnection().close();
    }

    @Override
    public boolean isClosed() {
        return getConnection().isClosed();
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        return getConnection().getTableBuilder(tableName, executorService);
    }

    @Override
    public void abort(String s, Throwable throwable) {
        getConnection().abort(s, throwable);
    }

    @Override
    public boolean isAborted() {
        return getConnection().isAborted();
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        return getConnection().getTable(tableName);
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        return getConnection().getTable(tableName, pool);
    }
}
