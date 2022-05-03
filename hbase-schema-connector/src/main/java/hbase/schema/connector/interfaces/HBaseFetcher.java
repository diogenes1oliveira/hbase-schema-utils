package hbase.schema.connector.interfaces;

import hbase.schema.connector.models.HBaseResultRow;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Interface to query and parse Java objects in HBase
 *
 * @param <Q> query type
 * @param <R> result type
 */
public interface HBaseFetcher<Q, R> {
    /**
     * Builds a Get query based on the data from a Java object
     *
     * @param query query object
     * @return built Get query object
     */
    Get toGet(Q query);

    /**
     * Builds, executes and parses Get requests
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    Stream<HBaseResultRow> get(Q query, TableName tableName, byte[] family, Get get);

    /**
     * Builds a list of Scan queries based on the data from a Java object
     *
     * @param query query object
     * @return built Scan query objects
     */
    List<Scan> toScans(Q query);

    /**
     * Default row batch size
     */
    int defaultRowBatchSize();

    /**
     * Executes multiple Scan requests sequentially
     *
     * @param query        query object
     * @param tableName    table to execute Get into
     * @param family       column family to fetch data from
     * @param rowBatchSize number of rows in a Scan batch
     * @return stream of raw results
     */
    Stream<List<HBaseResultRow>> scan(Q query, TableName tableName, byte[] family, List<Scan> scans, int rowBatchSize);

    /**
     * Parses data incoming from a HBase query
     *
     * @param query      original query object
     * @param resultRows HBase result objects
     * @return Stream with the valid parsed results
     */
    Stream<R> parseResults(Q query, List<HBaseResultRow> resultRows);

    /**
     * Executes one Get request
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream with one or zero raw results
     */
    default Stream<HBaseResultRow> get(Q query, String tableName, String family, Get get) {
        return get(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8), get);
    }

    /**
     * Builds, executes and parses Get requests
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Optional<R> getOptional(Q query, TableName tableName, byte[] family) {
        Get get = toGet(query);
        try (Stream<HBaseResultRow> stream = get(query, tableName, family, get)) {
            return stream.findFirst().flatMap(resultRow -> parseResult(query, resultRow));
        }
    }

    /**
     * Builds, executes and parses Get requests
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Optional<R> getOptional(Q query, String tableName, String family) {
        return getOptional(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Executes multiple Scan requests sequentially
     *
     * @param query        query object
     * @param tableName    table to execute Get into
     * @param family       column family to fetch data from
     * @param rowBatchSize number of rows in a Scan batch
     * @return stream of raw results
     */
    default Stream<List<HBaseResultRow>> scan(Q query, String tableName, String family, List<Scan> scans, int rowBatchSize) {
        return scan(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8), scans, rowBatchSize);
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Stream<R> scan(Q query, TableName tableName, byte[] family, int rowBatchSize) {
        List<Scan> scans = toScans(query);
        return scan(query, tableName, family, scans, rowBatchSize)
                .flatMap(results -> parseResults(query, results));
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Stream<R> scan(Q query, String tableName, String family, int rowBatchSize) {
        return scan(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8), rowBatchSize);
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default List<R> scanList(Q query, TableName tableName, byte[] family, int rowBatchSize) throws IOException {
        try (Stream<R> stream = scan(query, tableName, family, rowBatchSize)) {
            return stream.collect(toList());
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default List<R> scanList(Q query, String tableName, String family, int rowBatchSize) throws IOException {
        return scanList(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8), rowBatchSize);
    }

    /**
     * Executes multiple Scan requests sequentially
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of raw results
     */
    default Stream<List<HBaseResultRow>> scan(Q query, String tableName, String family, List<Scan> scans) {
        return scan(query, tableName, family, scans, defaultRowBatchSize());
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Stream<R> scan(Q query, TableName tableName, byte[] family) {
        return scan(query, tableName, family, defaultRowBatchSize());
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Stream<R> scan(Q query, String tableName, String family) {
        return scan(query, tableName, family, defaultRowBatchSize());
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default List<R> scanList(Q query, TableName tableName, byte[] family) throws IOException {
        return scanList(query, tableName, family, defaultRowBatchSize());
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute the Scan into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default List<R> scanList(Q query, String tableName, String family) throws IOException {
        return scanList(query, tableName, family, defaultRowBatchSize());
    }

    /**
     * Parses data incoming from a HBase query
     *
     * @param query     original query object
     * @param resultRow HBase result object
     * @return Optional with the parsed result or empty if nothing could be parsed
     */
    default Optional<R> parseResult(Q query, HBaseResultRow resultRow) {
        return parseResults(query, singletonList(resultRow)).findFirst();
    }


}
