package hbase.connector.interfaces;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Generic Proxy (as in the design pattern) for a HBase connection
 */
public class HBaseConnectionProxy implements Connection {
    private final Connection connection;

    public HBaseConnectionProxy(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Configuration getConfiguration() {
        return connection.getConfiguration();
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return connection.getBufferedMutator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams) throws IOException {
        return connection.getBufferedMutator(bufferedMutatorParams);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        return connection.getRegionLocator(tableName);
    }

    @Override
    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed();
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
        return connection.getTableBuilder(tableName, executorService);
    }

    @Override
    public void abort(String s, Throwable throwable) {
        connection.abort(s, throwable);
    }

    @Override
    public boolean isAborted() {
        return connection.isAborted();
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        return connection.getTable(tableName, pool);
    }

    @Override
    public void clearRegionLocationCache() {
        connection.clearRegionLocationCache();
    }

    @Override
    public Hbck getHbck() throws IOException {
        return connection.getHbck();
    }

    @Override
    public Hbck getHbck(ServerName masterServer) throws IOException {
        return connection.getHbck(masterServer);
    }
}
