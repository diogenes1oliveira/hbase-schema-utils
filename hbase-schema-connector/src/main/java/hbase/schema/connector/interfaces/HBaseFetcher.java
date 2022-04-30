package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Interface to query and parse Java objects in HBase
 *
 * @param <Q> query type
 * @param <R> result type
 */
public interface HBaseFetcher<Q, R> {
    /**
     * Builds, executes and parses Get requests
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    Stream<R> get(Q query, TableName tableName, byte[] family);

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Stream<R> get(Q query, String tableName, String family) {
        return get(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    Stream<R> scan(Q query, TableName tableName, byte[] family);

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    default Stream<R> scan(Q query, String tableName, String family) {
        return scan(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Parses data incoming from a HBase query
     *
     * @param query       original query object
     * @param family      column family the result came from
     * @param hBaseResult HBase result object
     * @return Optional with the parsed result or empty if nothing could be parsed
     */
    Optional<R> parseResult(Q query, byte[] family, Result hBaseResult);
}
