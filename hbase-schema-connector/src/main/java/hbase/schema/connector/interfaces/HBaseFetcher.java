package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
     * @param query query object
     * @return non-null results
     * @throws IOException failed to execute Get
     */
    List<R> get(Q query, TableName tableName, byte[] family) throws IOException;

    /**
     * Builds, executes and parses a Scan request
     *
     * @param query query object
     * @return list with non-null results
     * @throws IOException failed to execute Scan
     */
    List<R> scan(Q query, TableName tableName, byte[] family) throws IOException;

    default List<R> get(Q query, String tableName, String family) throws IOException {
        return get(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8));
    }

    default List<R> scan(Q query, String tableName, String family) throws IOException {
        return scan(query, TableName.valueOf(tableName), family.getBytes(StandardCharsets.UTF_8));
    }
}
