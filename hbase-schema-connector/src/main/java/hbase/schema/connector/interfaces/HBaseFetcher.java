package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.jetbrains.annotations.Nullable;

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
     * Builds a Get request
     *
     * @param query query object
     * @return built Get request or null if the query object has no query data
     */
    @Nullable
    Get toGet(Q query);

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
     * Parses the data from a HBase result into a proper object
     *
     * @param result fetched HBase result
     * @return parsed result object or null
     */
    @Nullable R parseResult(Result result);

    /**
     * Builds a Scan request
     *
     * @param queries query objects
     * @return built Scan request
     */
    Scan toScan(List<? extends Q> queries);

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
