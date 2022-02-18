package com.github.diogenes1oliveira.hbase.schema.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;
import java.util.SortedSet;

/**
 * Interface to parse data from a HBase Result into a POJO object
 *
 * @param <T> POJO type
 */
public interface HBaseReadSchema<T> extends HBaseFilterGenerator<T> {
    /**
     * Object to populate the POJO with data from the fetched row key
     *
     * @return row key parser
     */
    HBaseBytesParser<T> getRowKeyParser();

    /**
     * Object to generate the Get row key
     *
     * @return row key generator
     */
    HBaseBytesExtractor<T> getRowKeyGenerator();

    /**
     * Object to generate the Scan row key prefix
     *
     * @return search key prefix generator
     */
    HBaseBytesExtractor<T> getScanRowKeyGenerator();

    /**
     * Object to populate the POJO with data from the fetched qualifiers and values
     *
     * @return cells parser
     */
    List<HBaseBytesParser<T>> getCellsParsers();

    /**
     * Set of fixed qualifiers to read data from in a Get or a Put
     *
     * @return sorted set of qualifier bytes
     */
    SortedSet<byte[]> getQualifiers(T pojo);

    /**
     * Returns the common prefix to all qualifiers in the list returned by {@link #getQualifiers(T)}
     * <p>
     * The default implementation returns null, i.e., no common prefix for all qualifiers
     */
    @Nullable
    default byte[] getQualifiersPrefix(T pojo) {
        return null;
    }

    /**
     * Generates a filter based on the data from a POJO object
     * <p>
     * The default implementation generates a {@link ColumnPrefixFilter} based on the qualifier prefix
     * generated from {@link HBaseReadSchema#getQualifiersPrefix(T)}
     *
     * @param query POJO object to act as query source data
     * @return built filter or null
     */
    @Nullable
    @Override
    default Filter toFilter(T query) {
        byte[] prefix = this.getQualifiersPrefix(query);
        if (prefix == null) {
            return null;
        }
        return new ColumnPrefixFilter(prefix);
    }

}
