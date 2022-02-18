package com.github.diogenes1oliveira.hbase.schema.interfaces;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singleton;

/**
 * Interface to effectively build, execute and parse a Scan POJO query
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBasePojoScanner<T> {
    /**
     * Scans HBase for the data corresponding to the input POJOs
     *
     * @param queries       POJO objects to act as query source data
     * @param scanGenerator object to map the query data from the POJO into a Scan request
     * @return list of fetched POJOs
     * @throws IOException HBase error while scanning
     */
    List<T> scan(Collection<? extends T> queries, HBaseScanGenerator<T> scanGenerator) throws IOException;

    /**
     * Queries HBase for the data corresponding to the input POJO
     * <p>
     * The default implementation just forwards to {@link #scan(Collection, HBaseScanGenerator)}
     *
     * @param query         POJO object to act as query source data
     * @param scanGenerator object to map the query data from the POJO into a Scan request
     * @return list of fetched POJOs
     * @throws IOException HBase error while scanning
     */
    default List<T> scan(T query, HBaseScanGenerator<T> scanGenerator) throws IOException {
        return scan(singleton(query), scanGenerator);
    }
}
