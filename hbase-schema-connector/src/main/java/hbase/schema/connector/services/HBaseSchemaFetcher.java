package hbase.schema.connector.services;

import hbase.base.interfaces.IOSupplier;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.models.HBaseValueCell;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFilterBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaFetcher.class);

    private final TableName tableName;
    private final byte[] family;
    private final HBaseFilterBuilder<Q> filterBuilder;
    private final HBaseResultParser<R> resultParser;
    private final IOSupplier<Connection> connectionSupplier;

    public HBaseSchemaFetcher(String tableName,
                              byte[] family,
                              HBaseFilterBuilder<Q> filterBuilder,
                              HBaseResultParser<R> resultParser,
                              IOSupplier<Connection> connectionSupplier) {
        this.tableName = TableName.valueOf(tableName);
        this.family = family;
        this.filterBuilder = filterBuilder;
        this.resultParser = resultParser;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public List<R> get(List<? extends Q> queries) throws IOException {
        LOGGER.debug("Building gets for queries {}", queries);
        List<Get> gets = toGets(queries);
        if (queries.isEmpty()) {
            LOGGER.debug("No Get was created, returning empty list straight away");
            return emptyList();
        }
        LOGGER.debug("Get objects were created: {}, now connecting to HBase to execute them", gets);

        try (Connection connection = connectionSupplier.get();
             Table table = connection.getTable(tableName)) {
            Result[] results = table.get(gets);
            LOGGER.debug("Got results: {}", (Object[]) results);
            if (results == null) {
                return emptyList();
            }
            LOGGER.debug("Will now iterate through the Get results");
            return parseResults(Arrays.stream(results).iterator());
        } finally {
            LOGGER.debug("Gets finalized");
        }
    }

    @Override
    public List<R> scan(List<? extends Q> queries) throws IOException {
        LOGGER.debug("Building scan for queries {}", queries);
        Scan scan = toScan(queries);
        LOGGER.debug("Scan object was created: {}, now connecting to HBase to execute it", scan);

        try (Connection connection = connectionSupplier.get();
             Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)) {
            if (scanner == null) {
                LOGGER.debug("Result scanner is null, returning an empty list");
                return emptyList();
            }
            LOGGER.debug("Will now iterate through the Scan results");
            return parseResults(scanner.iterator());
        } finally {
            LOGGER.debug("Scan finalized");
        }
    }

    /**
     * Builds Get requests
     *
     * @param queries query objects
     * @return list of non-null Get requests
     */
    public List<Get> toGets(List<? extends Q> queries) {
        Filter filter = filterBuilder.toFilter(queries);
        List<Get> gets = new ArrayList<>();

        for (Q query : queries) {
            byte[] rowKey = filterBuilder.toRowKey(query);
            if (rowKey == null) {
                continue;
            }
            Get get = new Get(rowKey);
            filterBuilder.selectColumns(query, family, get);
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
        int count = 0;

        LOGGER.debug("Iterating through the results");
        while (hBaseResults.hasNext()) {
            LOGGER.debug("Getting the next result");
            Result hBaseResult = hBaseResults.next();
            LOGGER.debug("Got one result: {}", hBaseResult);
            ++count;
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

        LOGGER.debug("Parsed {} from {} rows", results.size(), count);
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
            LOGGER.debug("Scan without any filter, for there are no queries");
            scan.addFamily(family);
            return scan;
        }

        Q firstQuery = queries.get(0);
        filterBuilder.selectColumns(firstQuery, family, scan);

        Filter scanFilter = filterBuilder.toMultiRowRangeFilter(queries);
        LOGGER.debug("Row range filter: {}", scanFilter);
        Filter genericFilter = filterBuilder.toFilter(queries);
        LOGGER.debug("Generic filter: {}", scanFilter);
        Filter filter = combineNullableFilters(FilterList.Operator.MUST_PASS_ALL, scanFilter, genericFilter);

        if (filter != null) {
            scan.setFilter(filter);
            LOGGER.debug("Scan filter: {}", filter);
        } else {
            LOGGER.debug("Scan without filter");
        }

        return scan;
    }

}
