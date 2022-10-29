package hbase.schema.api.testutils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class HBaseDml implements AutoCloseable {
    private final Table table;
    private final byte[] family;

    public HBaseDml(Connection connection, String tableName, String family) {
        this(connection, TableName.valueOf(tableName), family);
    }

    public HBaseDml(Connection connection, TableName tableName, String family) {
        this.family = family.getBytes(StandardCharsets.UTF_8);

        try {
            this.table = connection.getTable(tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void put(String rowKey, String... qualifiersAndValues) {
        Put put = new Put(rowKey.getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < qualifiersAndValues.length; i += 2) {
            byte[] qualifier = qualifiersAndValues[i].getBytes(StandardCharsets.UTF_8);
            byte[] value = qualifiersAndValues[i + 1].getBytes(StandardCharsets.UTF_8);
            put.addColumn(family, qualifier, value);
        }
        try {
            table.put(put);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void increment(String rowKey, Object... qualifiersAndValues) {
        Increment inc = new Increment(rowKey.getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < qualifiersAndValues.length; i += 2) {
            byte[] qualifier = ((String) qualifiersAndValues[i]).getBytes(StandardCharsets.UTF_8);
            long delta = (Long) qualifiersAndValues[i + 1];
            inc.addColumn(family, qualifier, delta);
        }
        try {
            table.increment(inc);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Result get(String rowKey) {
        Get get = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
        get.addFamily(family);
        try {
            return table.get(get);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.table.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
