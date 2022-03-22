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
     * @param queries query objects
     * @return non-null results
     * @throws IOException failed to execute Get
     */
    List<R> get(List<? extends Q> queries) throws IOException;

    /**
     * Builds, executes and parses a Scan request
     *
     * @param queries query objects
     * @return list with non-null results
     * @throws IOException failed to execute Get
     */
    List<R> scan(List<? extends Q> queries) throws IOException;
}
