package hbase.schema.api.connector;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.HBaseSchema;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class HBaseSchemaConnector<T, R> {
    private final TableName tableName;
    private final byte[] family;
    private final HBaseConnector connector;
    private final HBaseQuerySchema<T> querySchema;
    private final HBaseMutationSchema<T> mutationSchema;
    private final HBaseResultParser<R> resultParser;

    public HBaseSchemaConnector(TableName tableName,
                                byte[] family,
                                HBaseConnector connector,
                                HBaseSchema<T, R> schema) {
        this.tableName = tableName;
        this.family = family;
        this.connector = connector;
        this.querySchema = schema.querySchema();
        this.mutationSchema = schema.mutationSchema();
        this.resultParser = schema.resultParser();
    }

    public List<R> get(List<? extends T> queries) throws IOException {
        List<Get> gets = queries.stream().map(query -> querySchema.toGet(query, family)).collect(toList());

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName)
        ) {
            Result[] results = table.get(gets);
            if (results == null) {
                return emptyList();
            }
            return stream(results).map(this::parse).collect(toList());
        }
    }

    public List<R> scan(List<? extends T> queries) throws IOException {
        Scan scan = querySchema.toScan(queries, family);
        List<R> objs = new ArrayList<>();

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)
        ) {
            if (scanner == null) {
                return emptyList();
            }
            for (Result result : scanner) {
                R obj = parse(result);
                if (obj != null) {
                    objs.add(obj);
                }
            }
        }

        return objs;
    }

    public void mutate(Collection<? extends T> objs) throws IOException {
        List<Mutation> mutations = new ArrayList<>();

        for (T obj : objs) {
            Put put = mutationSchema.toPut(obj, family);
            if (put != null) {
                mutations.add(put);
            }
            Increment increment = mutationSchema.toIncrement(obj, family);
            if (increment != null) {
                mutations.add(increment);
            }
        }

        if (mutations.isEmpty()) {
            throw new IllegalArgumentException("No mutations to execute");
        }

        Result[] results = new Result[mutations.size()];

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName)
        ) {
            table.batch(mutations, results);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while mutating", e);
        }
    }


    @Nullable
    public R parse(Result result) {
        if (result == null) {
            return null;
        }
        byte[] rowKey = result.getRow();
        if (rowKey == null) {
            return null;
        }
        SortedMap<byte[], byte[]> cellsMap = result.getFamilyMap(family);
        if (cellsMap == null) {
            return null;
        }
        R obj = resultParser.newInstance();

        resultParser.setFromRowKey(obj, result.getRow());
        resultParser.setFromResult(obj, result.getFamilyMap(family));
        return obj;
    }

}
