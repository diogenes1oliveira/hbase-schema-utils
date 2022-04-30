package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <Q> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaFetcher.class);

    private final HBaseConnector connector;
    private final HBaseReadSchema<Q, R> readSchema;

    public HBaseSchemaFetcher(HBaseConnector connector, HBaseReadSchema<Q, R> readSchema) {
        this.connector = connector;
        this.readSchema = readSchema;
    }

    @Override
    public List<R> get(Q query, TableName tableName, byte[] family) throws IOException {
        LOGGER.debug("Building Get for query {}", query);
        Get get = readSchema.toGet(query);
        get.addFamily(family);

        LOGGER.debug("Get object was created: {}, now connecting to HBase to execute it", get);

        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName)) {
            Result result = table.get(get);
            LOGGER.debug("Got result, will now parse it: {}", result);
            return parseResults(query, family, singleton(result).iterator());
        } finally {
            LOGGER.debug("Get finalized");
        }
    }

    @Override
    public List<R> scan(Q query, TableName tableName, byte[] family) throws IOException {
        LOGGER.debug("Building scan for queries {}", query);
        Scan scan = readSchema.toScan(query);
        LOGGER.debug("Scan object was created: {}, now connecting to HBase to execute it", scan);

        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)) {
            if (scanner == null) {
                LOGGER.debug("Result scanner is null, returning an empty list");
                return emptyList();
            }
            LOGGER.debug("Will now iterate through the Scan results");
            return parseResults(query, family, scanner.iterator());
        } finally {
            LOGGER.debug("Scan finalized");
        }
    }

    private List<R> parseResults(Q query, byte[] family, Iterator<Result> hBaseResults) {
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
            R result = readSchema.newInstance();
            boolean parsed = readSchema.parseRowKey(result, ByteBuffer.wrap(rowKey), query);
            for (Map.Entry<byte[], byte[]> entry : hBaseResult.getFamilyMap(family).entrySet()) {
                byte[] qualifier = entry.getKey();
                byte[] value = entry.getValue();
                if (value != null) {
                    parsed = readSchema.parseCell(result, ByteBuffer.wrap(qualifier), ByteBuffer.wrap(value), query) || parsed;
                }
            }
            if (parsed) {
                results.add(result);
            }
        }

        LOGGER.info("Parsed {} result from {} rows", results.size(), count);
        return results;
    }

}
