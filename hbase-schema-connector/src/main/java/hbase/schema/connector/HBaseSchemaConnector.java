package hbase.schema.connector;

import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.connector.interfaces.HBasePojoGetBuilder;
import hbase.schema.connector.interfaces.HBasePojoMutationBuilder;
import hbase.schema.connector.interfaces.HBasePojoScanBuilder;
import hbase.schema.connector.interfaces.HBaseResultParser;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HBaseSchemaConnector<T> implements HBasePojoScanBuilder<T>, HBasePojoGetBuilder<T>, HBasePojoMutationBuilder<T>, HBaseResultParser<T> {
    private final HBaseConnector connector;
    private final TableName tableName;
    private final byte[] family;
    private final HBaseReadSchema<T> readSchema;
    private final HBaseWriteSchema<T> writeSchema;

    public HBaseSchemaConnector(HBaseReadSchema<T> readSchema,
                                HBaseWriteSchema<T> writeSchema,
                                HBaseConnector connector,
                                TableName tableName,
                                byte[] family) {
        this.readSchema = readSchema;
        this.writeSchema = writeSchema;
        this.connector = connector;
        this.tableName = tableName;
        this.family = family;
    }


    public List<T> get(List<Get> gets) throws IOException {
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

    public Optional<T> get(Get get) throws IOException {
        List<T> result = this.get(singletonList(get));
        if (result.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(result.get(0));
        }
    }

    public List<T> scan(Scan scan) throws IOException {
        List<T> objs = new ArrayList<>();

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)
        ) {
            if (scanner == null) {
                return emptyList();
            }
            for (Result result : scanner) {
                T obj = parse(result);
                if (obj != null) {
                    objs.add(obj);
                }
            }
        }

        return objs;
    }

    public void mutate(List<Mutation> mutations) throws IOException {
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

    @Override
    public T parse(Result result) {
        if (result == null) {
            return null;
        }
        byte[] rowKey = result.getRow();
        SortedMap<byte[], byte[]> cellsMap = result.getFamilyMap(family);
        if (rowKey == null) {
            return null;
        }
        T obj = readSchema.newInstance();
        readSchema.getRowKeyParser().setFromBytes(obj, rowKey);

        parse(obj, rowKey, cellsMap, readSchema);
        return obj;
    }

    @Override
    public List<Mutation> toMutations(T obj) {
        return toMutations(obj, family, writeSchema);
    }

    @Override
    public Get toGet(T query) {
        byte[] rowKey = requireNonNull(readSchema.getRowKeyGenerator().getBytes(query));
        Get get = new Get(rowKey);
        selectColumns(get, query, family, readSchema);

        Filter filter = toFilter(query, readSchema);
        if (filter != null) {
            get.setFilter(filter);
        }
        return get;
    }

    @Override
    public Scan toScan(List<? extends T> queries) {
        Scan scan = new Scan();
        selectColumns(scan, queries.get(0), family, readSchema);

        Filter filter = toFilter(queries, family, readSchema);
        if (filter != null) {
            scan.setFilter(filter);
        }
        return scan;
    }
}
