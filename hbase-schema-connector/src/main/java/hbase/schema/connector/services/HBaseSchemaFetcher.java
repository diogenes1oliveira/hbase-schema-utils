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
        LOGGER.info("Building Get for query {}", query);
        Get get = readSchema.toGet(query);
        get.addFamily(family);

        LOGGER.info("Get object was created: {}, now connecting to HBase to execute it", get);

        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName)) {
            Result result = table.get(get);
            LOGGER.info("Got result, will now parse it: {}", result);
            return parseResults(query, family, singleton(result).iterator());
        } finally {
            LOGGER.info("Get finalized");
        }
    }

    @Override
    public List<R> scan(Q query, TableName tableName, byte[] family) throws IOException {
        LOGGER.info("Building scans for queries {}", query);
        List<Scan> scans = readSchema.toScans(query);
        LOGGER.info("Scan objects were created: {}, now connecting to HBase to execute them", scans);

        List<R> results = new ArrayList<>();

        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName)) {
            for (Scan scan : scans) {
                results.addAll(scanResults(query, family, table, scan));
            }
        } finally {
            LOGGER.info("Scans finalized");
        }

        return results;
    }

    private List<R> scanResults(Q query, byte[] family, Table table, Scan scan) throws IOException {
        scan.addFamily(family);
        LOGGER.info("Now executing Scan {}", scan);

        try (ResultScanner scanner = table.getScanner(scan)) {
            if (scanner == null) {
                LOGGER.info("Result scanner for scan {} is null, returning an empty list", scan);
                return emptyList();
            }
            LOGGER.info("Will now iterate through the Scan results");
            return parseResults(query, family, scanner.iterator());
        }
    }

    private List<R> parseResults(Q query, byte[] family, Iterator<Result> hBaseResults) {
        List<R> results = new ArrayList<>();
        int count = 0;

        LOGGER.info("Iterating through the results");
        while (hBaseResults.hasNext()) {
            LOGGER.info("Getting the next result");
            Result hBaseResult = hBaseResults.next();
            LOGGER.info("Got one result: {}", hBaseResult);
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
