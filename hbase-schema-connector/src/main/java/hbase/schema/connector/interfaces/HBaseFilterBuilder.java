package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface HBaseFilterBuilder<T> {
    /**
     * Builds a row key to be used in Get requests
     *
     * @param query input query object
     * @return row key {@code byte[]}
     */
    byte @Nullable [] toRowKey(T query);

    /**
     * Builds a MultiRowRangeFilter to be used in Scan requests
     *
     * @param queries input query objects
     * @return multi row range filter
     */
    MultiRowRangeFilter toMultiRowRangeFilter(List<? extends T> queries);

    /**
     * Builds the filter to be used in Scan requests
     * <p>
     * The default implementation just returns {@code null}
     *
     * @param queries input query objects
     * @return filter for Scan requests
     */
    default @Nullable Filter toFilter(List<? extends T> queries) {
        return null;
    }

    /**
     * Selects the columns returned in a Get query
     *
     * @param query  query object
     * @param family column family
     * @param get    HBase Get instance
     */
    void selectColumns(T query, byte[] family, Get get);

    /**
     * Selects the columns returned in a Scan query
     *
     * @param query  query object
     * @param family column family
     * @param scan   HBase Scan instance
     */
    void selectColumns(T query, byte[] family, Scan scan);
}
