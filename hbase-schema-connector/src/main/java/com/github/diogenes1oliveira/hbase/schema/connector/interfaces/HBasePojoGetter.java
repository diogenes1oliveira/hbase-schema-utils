package com.github.diogenes1oliveira.hbase.schema.connector.interfaces;

import com.github.diogenes1oliveira.hbase.schema.api.interfaces.HBaseGetGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.singleton;

/**
 * Interface to effectively build, execute and parse a Get POJO query
 */
@FunctionalInterface
public interface HBasePojoGetter {
    /**
     * Queries HBase for the data corresponding to the input POJOs
     *
     * @param queries      POJO objects to act as query source data
     * @param getGenerator object to map the query data from the POJO into a Get request
     * @param <T>          POJO type
     * @return list of fetched POJOs
     * @throws IOException HBase error while fetching
     */
    <T> List<T> get(Collection<? extends T> queries, HBaseGetGenerator<T> getGenerator) throws IOException;

    /**
     * Queries HBase for the data corresponding to the input POJO
     * <p>
     * The default implementation just forwards to {@link #get(Collection, HBaseGetGenerator)}
     *
     * @param query        POJO object to act as query source data
     * @param getGenerator object to map the query data from the POJO into a Get request
     * @param <T>          POJO type
     * @return fetched POJO or an empty optional
     * @throws IOException           HBase error while mutating
     * @throws IllegalStateException more than 1 result returned by query
     */
    default <T> Optional<T> get(T query, HBaseGetGenerator<T> getGenerator) throws IOException {
        List<T> pojos = get(singleton(query), getGenerator);

        if (pojos.isEmpty()) {
            return Optional.empty();
        } else if (pojos.size() == 1) {
            return Optional.of(pojos.get(0));
        } else {
            throw new IllegalStateException("Too many results returned by Get");
        }
    }
}
