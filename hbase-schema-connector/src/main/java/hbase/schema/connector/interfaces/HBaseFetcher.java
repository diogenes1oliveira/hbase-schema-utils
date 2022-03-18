package hbase.schema.connector.interfaces;

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
     * Builds, executes and parses Get requests
     *
     * @param tableName name of the table to query data from
     * @param queries   query objects
     * @return non-null results
     * @throws IOException failed to execute Get
     */
    List<R> get(String tableName, List<? extends Q> queries) throws IOException;

    /**
     * Builds, executes and parses a Scan request
     *
     * @param tableName name of the table to query data from
     * @param queries   query objects
     * @return list with non-null results
     * @throws IOException failed to execute Get
     */
    List<R> scan(String tableName, List<? extends Q> queries) throws IOException;
}
