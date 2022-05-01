package hbase.schema.connector.services;

import hbase.connector.services.HBaseStreamFetcher;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.utils.HBaseQueryUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <Q> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaFetcher.class);

    private final HBaseReadSchema<Q, R> readSchema;
    private final HBaseStreamFetcher streamFetcher;

    public HBaseSchemaFetcher(HBaseStreamFetcher streamFetcher, HBaseReadSchema<Q, R> readSchema) {
        this.streamFetcher = streamFetcher;
        this.readSchema = readSchema;
    }

    @Override
    public Get toGet(Q query) {
        LOGGER.info("Building Get for query {}", query);
        return readSchema.toGet(query);
    }

    @Override
    public Stream<R> get(Q query, TableName tableName, byte[] family, Get get) {
        LOGGER.info("Get object was created: {}, now connecting to HBase to execute it", get);

        return streamFetcher.fetch(tableName, get)
                            .map(result -> parseResult(query, family, result))
                            .flatMap(HBaseQueryUtils::optionalToStream);
    }

    @Override
    public List<Scan> toScans(Q query) {
        LOGGER.info("Building scans for queries {}", query);
        return readSchema.toScans(query);
    }

    @Override
    public Stream<Result> scan(Q query, TableName tableName, byte[] family, List<Scan> scans) {
        for (Scan scan : scans) {
            scan.addFamily(family);
        }
        LOGGER.info("Scan objects were created: {}, now connecting to HBase to execute them", scans);

        return streamFetcher.fetch(tableName, scans);
    }

    @Override
    public Optional<R> parseResult(Q query, byte[] family, Result hBaseResult) {
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
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }


}
