package hbase.schema.connector.services;

import hbase.connector.services.HBaseStreamFetcher;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.models.HBaseResultRow;
import hbase.schema.connector.utils.HBaseQueryUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static hbase.schema.connector.models.HBaseResultRow.fromResult;
import static hbase.schema.connector.models.HBaseResultRow.fromResults;

/**
 * Object to query and parse data from HBase based on a Schema
 *
 * @param <Q> query type
 * @param <R> result type
 */
public class HBaseSchemaFetcher<Q, R> implements HBaseFetcher<Q, R> {
    public static final int DEFAULT_ROW_BATCH_SIZE = 500;

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
    public Stream<HBaseResultRow> get(Q query, TableName tableName, byte[] family, Get get) {
        get.addFamily(family);
        LOGGER.info("Get object was created: {}, now connecting to HBase to execute it", get);

        return streamFetcher.fetch(tableName, get)
                            .map(result -> fromResult(family, result));
    }

    @Override
    public List<Scan> toScans(Q query) {
        LOGGER.debug("Building scans for queries {}", query);
        return readSchema.toScans(query);
    }

    @Override
    public int defaultRowBatchSize() {
        return DEFAULT_ROW_BATCH_SIZE;
    }

    @Override
    public Stream<List<HBaseResultRow>> scan(Q query, TableName tableName, byte[] family, List<Scan> scans, int rowBatchSize) {
        for (Scan scan : scans) {
            scan.addFamily(family);
        }
        return streamFetcher.fetch(tableName, scans, rowBatchSize)
                            .map(results -> fromResults(family, results));
    }

    @Override
    public Stream<R> parseResults(Q query, List<HBaseResultRow> resultRows) {
        return resultRows.stream()
                         .map(resultRow -> parseResult(query, resultRow))
                         .flatMap(HBaseQueryUtils::optionalToStream);
    }

    @Override
    public Optional<R> parseResult(Q query, HBaseResultRow resultRow) {
        R result = readSchema.newInstance();

        boolean parsed = readSchema.parseRowKey(result, resultRow.rowKey(), query);

        for (Map.Entry<byte[], byte[]> entry : resultRow.cellsMap().entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] value = entry.getValue();
            if (value != null) {
                parsed = readSchema.parseCell(result, ByteBuffer.wrap(qualifier), ByteBuffer.wrap(value), query) || parsed;
            }
        }

        if (parsed && readSchema.validate(result, query)) {
            return Optional.of(result);
        } else {
            LOGGER.info("Invalid unparseable result {}", resultRow);
            return Optional.empty();
        }
    }

}
