package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseFilterSchema;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <Q> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private final byte[] family;
    private final HBaseFilterSchema<Q> filterSchema;
    private final HBaseQuerySchema<Q> querySchema;
    private final HBaseResultParserSchema<R> resultParserSchema;
    private final HBaseConnector connector;

    /**
     * @param family    column family
     * @param schema    object schema
     * @param connector connector object
     */
    public HBaseSchemaFetcher(byte[] family, HBaseSchema<?, Q, R> schema, HBaseConnector connector) {
        this.family = family;
        this.filterSchema = schema.filterSchema();
        this.querySchema = schema.querySchema();
        this.resultParserSchema = schema.resultParserSchema();
        this.connector = connector;
    }

    /**
     * Builds a Get request
     *
     * @param query query object
     * @return built Get request or null if the query object has no query data
     */
    @Nullable
    public Get toGet(Q query) {
        byte[] rowKey = querySchema.buildRowKey(query);
        if (rowKey == null) {
            return null;
        }

        Get get = new Get(rowKey);
        querySchema.selectColumns(query, family, get);

        Filter filter = filterSchema.buildFilter(query);
        if (filter != null) {
            get.setFilter(filter);
        }

        return get;
    }

    /**
     * Builds, executes and parses a Get request
     *
     * @param tableName name of the table to query data in
     * @param query     query object
     * @return found result or null
     * @throws IOException failed to execute Get
     */
    @Nullable
    public R get(TableName tableName, Q query) throws IOException {
        Get get = toGet(query);
        if (get == null) {
            return null;
        }
        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName)) {
            Result result = table.get(get);
            return parseResult(result);
        }
    }

    /**
     * Parses the data from a HBase result into a proper object
     *
     * @param result fetched HBase result
     * @return parsed result object or null
     */
    public @Nullable R parseResult(Result result) {
        if (result == null) {
            return null;
        }
        byte[] rowKey = result.getRow();
        NavigableMap<byte[], byte[]> cellsMap = result.getFamilyMap(family);

        if (rowKey == null || cellsMap == null) {
            return null;
        }
        return resultParserSchema.parseResult(rowKey, cellsMap);
    }

    /**
     * Builds a Scan request
     *
     * @param queries query objects
     * @return built Scan request
     */
    public Scan toScan(List<? extends Q> queries) {
        Scan scan = new Scan();

        if (queries.isEmpty()) {
            scan.addFamily(family);
            return scan;
        }

        Q firstQuery = queries.get(0);
        querySchema.selectColumns(firstQuery, family, scan);

        Filter filter = filterSchema.buildFilter(queries);
        if (filter != null) {
            scan.setFilter(filter);
        }

        return scan;
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param tableName name of the table to query data in
     * @param queries   query objects
     * @return list with non-null results
     * @throws IOException failed to execute Get
     */
    public List<R> scan(TableName tableName, List<? extends Q> queries) throws IOException {
        Scan scan = toScan(queries);
        List<R> results = new ArrayList<>();

        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                R object = parseResult(result);
                if (object != null) {
                    results.add(object);
                }
            }
        }

        return results;
    }
}
