package hbase.schema.connector.services;

import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFilterGenerator;
import hbase.schema.connector.interfaces.HBaseResultsParser;
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
import java.util.List;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <T> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<T, R> implements HBaseFetcher<T, R> {
    private final byte[] family;
    private final HBaseFilterGenerator<T> filterGenerator;
    private final HBaseResultsParser<R> resultsParser;
    private final HBaseQuerySchema<T> querySchema;
    private final HBaseConnector connector;

    /**
     * @param family    column family
     * @param schema    object schema
     * @param connector connector object
     */
    public HBaseSchemaFetcher(byte[] family, HBaseSchema<T, R> schema, HBaseConnector connector) {
        this.family = family;
        this.filterGenerator = new HBaseSchemaFilterGenerator<>(family, schema.querySchema());
        this.resultsParser = new HBaseSchemaResultsParser<>(family, schema.resultParserSchema());
        this.querySchema = schema.querySchema();
        this.connector = connector;
    }

    /**
     * Builds a Get request
     *
     * @param query query object
     * @return built Get request or null if the query object has no query data
     */
    @Nullable
    public Get toGet(T query) {
        byte[] rowKey = querySchema.buildRowKey(query);
        if (rowKey == null) {
            return null;
        }
        Get get = new Get(rowKey);
        filterGenerator.selectColumns(query, get);
        Filter filter = filterGenerator.toFilter(query);
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
    public R get(TableName tableName, T query) throws IOException {
        Get get = toGet(query);
        if (get == null) {
            return null;
        }
        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName)) {
            Result result = table.get(get);
            return resultsParser.parseResult(result);
        }
    }

    /**
     * Builds a Scan request
     *
     * @param queries query objects
     * @return built Scan request
     */
    public Scan toScan(List<? extends T> queries) {
        Scan scan = new Scan();

        if (queries.isEmpty()) {
            scan.addFamily(family);
            return scan;
        }

        T firstQuery = queries.get(0);
        filterGenerator.selectColumns(firstQuery, scan);

        Filter filter = filterGenerator.toFilter(queries);
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
    public List<R> scan(TableName tableName, List<? extends T> queries) throws IOException {
        Scan scan = toScan(queries);
        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)) {
            return resultsParser.parseResults(scanner);
        }
    }
}
