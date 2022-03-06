package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;

/**
 * Interface to query and parse Java objects in HBase
 *
 * @param <Q> query type
 * @param <R> result type
 */
public interface HBaseFetcher<Q, R> {
    /**
     * Builds, executes and parses a Get request
     *
     * @param tableName name of the table to query data in
     * @param query     query object
     * @return found result or null
     * @throws IOException failed to execute Get
     */
    R get(TableName tableName, Q query) throws IOException;

    /**
     * Builds, executes and parses a Scan request
     *
     * @param tableName name of the table to query data in
     * @param queries   query objects
     * @return list with non-null results
     * @throws IOException failed to execute Get
     */
    List<R> scan(TableName tableName, List<? extends Q> queries) throws IOException;
}
