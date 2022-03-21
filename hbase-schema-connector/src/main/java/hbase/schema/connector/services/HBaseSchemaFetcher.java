package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.models.HBaseValueCell;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseQueryBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.connector.utils.HBaseQueryUtils.combineNullableFilters;
import static java.util.Collections.emptyList;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <Q> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private final byte[] family;
    private final HBaseQueryBuilder<Q> queryBuilder;
    private final HBaseResultParser<R> resultParser;
    private final HBaseConnector connector;

    /**
     * @param family    column family
     * @param schema    schema
     * @param connector connector object
     */
    public HBaseSchemaFetcher(byte[] family,
                              HBaseSchema<Q, R> schema,
                              HBaseConnector connector) {
        this.family = family;
        this.queryBuilder = new HBaseCellsQueryBuilder<>(schema.scanKeySize(), schema.mutationMapper());
        this.resultParser = schema.resultParser();
        this.connector = connector;
    }

    @Override
    public List<R> get(String tableName, List<? extends Q> queries) throws IOException {
        List<Get> gets = toGets(queries);
        if (queries.isEmpty()) {
            return emptyList();
        }

        try (Connection connection = connector.context();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Result[] results = table.get(gets);
            if (results == null) {
                return emptyList();
            }
            return parseResults(Arrays.stream(results).iterator());
        }
    }

    @Override
    public List<R> scan(String tableName, List<? extends Q> queries) throws IOException {
        Scan scan = toScan(queries);

        try (Connection connection = connector.context();
             Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(scan)) {
            if (scanner == null) {
                return emptyList();
            }
            return parseResults(scanner.iterator());
        }
    }

    /**
     * Builds Get requests
     *
     * @param queries query objects
     * @return list of non-null Get requests
     */
    public List<Get> toGets(List<? extends Q> queries) {
        Filter filter = queryBuilder.toFilter(queries);
        List<Get> gets = new ArrayList<>();

        for (Q query : queries) {
            byte[] rowKey = queryBuilder.toRowKey(query);
            if (rowKey == null) {
                continue;
            }
            Get get = new Get(rowKey);
            queryBuilder.selectColumns(query, family, get);
            if (filter != null) {
                get.setFilter(filter);
            }
            gets.add(get);
        }

        return gets;
    }

    public List<HBaseValueCell> toCells(Result result) {
        List<HBaseValueCell> cells = new ArrayList<>();
        if (result == null) {
            return cells;
        }
        NavigableMap<byte[], byte[]> cellsMap = result.getFamilyMap(family);
        if (cellsMap == null) {
            return cells;
        }
        for (Map.Entry<byte[], byte[]> entry : cellsMap.entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] value = entry.getValue();
            if (qualifier != null && value != null) {
                HBaseValueCell cell = new HBaseValueCell(qualifier, value);
                cells.add(cell);
            }
        }
        return cells;
    }

    public List<R> parseResults(Iterator<Result> hBaseResults) {
        List<R> results = new ArrayList<>();

        while (hBaseResults.hasNext()) {
            Result hBaseResult = hBaseResults.next();
            if (hBaseResult == null) {
                continue;
            }
            byte[] rowKey = hBaseResult.getRow();
            if (rowKey == null) {
                continue;
            }
            List<HBaseValueCell> cells = toCells(hBaseResult);
            R result = resultParser.newInstance();
            resultParser.parseRowKey(result, rowKey);
            resultParser.parseCells(result, cells);
            results.add(result);
        }

        return results;
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
        queryBuilder.selectColumns(firstQuery, family, scan);

        Filter scanFilter = queryBuilder.toMultiRowRangeFilter(queries);
        Filter genericFilter = queryBuilder.toFilter(queries);
        Filter filter = combineNullableFilters(FilterList.Operator.MUST_PASS_ALL, scanFilter, genericFilter);

        if (filter != null) {
            scan.setFilter(filter);
        }

        return scan;
    }

}
