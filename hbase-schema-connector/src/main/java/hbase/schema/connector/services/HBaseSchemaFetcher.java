package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <Q> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaFetcher.class);

    private final HBaseReadSchema<Q, R> readSchema;
    private final HBaseStreamScanner streamScanner;
    private final HBaseStreamGetter streamGetter;

    public HBaseSchemaFetcher(HBaseConnector connector, HBaseReadSchema<Q, R> readSchema) {
        this.readSchema = readSchema;

        this.streamScanner = new HBaseStreamScanner(connector);
        this.streamGetter = new HBaseStreamGetter(connector);
    }

    @Override
    public List<R> get(Q query, TableName tableName, byte[] family) throws IOException {
        LOGGER.info("Building Get for query {}", query);
        Get get = readSchema.toGet(query);
        get.addFamily(family);

        LOGGER.info("Get object was created: {}, now connecting to HBase to execute it", get);

        try {
            return streamGetter.fetch(tableName, get)
                               .flatMap(result -> parseResult(query, family, result))
                               .collect(toList());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @Override
    public List<R> scan(Q query, TableName tableName, byte[] family) throws IOException {
        LOGGER.info("Building scans for queries {}", query);
        List<Scan> scans = readSchema.toScans(query);
        for (Scan scan : scans) {
            scan.addFamily(family);
        }
        LOGGER.info("Scan objects were created: {}, now connecting to HBase to execute them", scans);

        try {
            return streamScanner.fetch(tableName, scans)
                                .flatMap(result -> parseResult(query, family, result))
                                .collect(toList());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

    }

    private Stream<R> parseResult(Q query, byte[] family, Result hBaseResult) {
        byte[] rowKey = hBaseResult.getRow();
        R result = readSchema.newInstance();
        boolean parsed = readSchema.parseRowKey(result, ByteBuffer.wrap(rowKey), query);
        for (Map.Entry<byte[], byte[]> entry : hBaseResult.getFamilyMap(family).entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] value = entry.getValue();
            if (value != null) {
                parsed = readSchema.parseCell(result, ByteBuffer.wrap(qualifier), ByteBuffer.wrap(value), query) || parsed;
            }
        }
        if (parsed && readSchema.validate(result, query)) {
            return Stream.of(result);
        } else {
            return Stream.empty();
        }
    }


}
