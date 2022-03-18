package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;

public interface HBaseQueryBuilder<T> {
    Set<byte[]> EMPTY = Collections.unmodifiableSet(asBytesTreeSet());

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
     * Gets the fixed qualifiers to be fetched in the query
     * <p>
     * The default implementation returns an empty set
     *
     * @param query input query object
     * @return this builder
     */
    @NotNull
    default Set<byte[]> getQualifiers(T query) {
        return EMPTY;
    }

    /**
     * Gets the qualifier prefixes to be fetched in the query
     * <p>
     * The default implementation returns an empty set
     *
     * @param query input query object
     * @return this builder
     */
    @NotNull
    default Set<byte[]> getPrefixes(T query) {
        return EMPTY;
    }

    /**
     * Selects the columns returned in a Get query
     * <p>
     * The default implementation:
     * <li>Selects the fixed columns in {@link #getQualifiers(Object)} if {@link #getPrefixes(Object)} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param get    HBase Get instance
     */
    default void selectColumns(T query, byte[] family, Get get) {
        if (getPrefixes(query).isEmpty()) {
            for (byte[] qualifier : getQualifiers(query)) {
                get.addColumn(family, qualifier);
            }
        } else {
            get.addFamily(family);
        }
    }

    /**
     * Selects the columns returned in a Scan query
     * <p>
     * The default implementation:
     * <li>Selects the fixed columns in {@link #getQualifiers(Object)} if {@link #getPrefixes(Object)} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param scan   HBase Scan instance
     */
    default void selectColumns(T query, byte[] family, Scan scan) {
        if (getPrefixes(query).isEmpty()) {
            for (byte[] qualifier : getQualifiers(query)) {
                scan.addColumn(family, qualifier);
            }
        } else {
            scan.addFamily(family);
        }

    }
}
