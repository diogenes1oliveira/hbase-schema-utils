package com.github.diogenes1oliveira.hbase.schema.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Interface to generate a Filter corresponding to a POJO query
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseFilterGenerator<T> {
    /**
     * Generates a filter based on the data from a POJO object
     *
     * @param query POJO object to act as query source data
     * @return built filter or null
     */
    @Nullable
    Filter toFilter(T query);

}
