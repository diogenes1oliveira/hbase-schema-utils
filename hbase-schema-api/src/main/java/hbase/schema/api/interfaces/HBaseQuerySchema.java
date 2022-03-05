package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;

/**
 * Interface to generate Get and Scan query data from a Java object
 *
 * @param <T> query object type
 */
public interface HBaseQuerySchema<T> {
    /**
     * Generates the row key to be used in a Get request
     *
     * @param query input query object
     * @return this builder
     */
    byte[] buildRowKey(T query);

    /**
     * Gets the fixed qualifiers to be fetched in the query
     * <p>
     * The default implementation returns an empty set
     *
     * @param query input query object
     * @return this builder
     */
    default SortedSet<byte[]> getQualifiers(T query) {
        return asBytesTreeSet();
    }

    /**
     * Gets the qualifier prefixes to be fetched in the query
     * <p>
     * The default implementation returns an empty set
     *
     * @param query input query object
     * @return this builder
     */
    default SortedSet<byte[]> getPrefixes(T query) {
        return asBytesTreeSet();
    }

    /**
     * Selects the columns returned in a Get query
     * <p>
     * The default implementation:
     * <li>Selects the fixed columns in {@link #getQualifiers(T)} if {@link #getPrefixes(T)} is empty;</li>
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
     * <li>Selects the fixed columns in {@link #getQualifiers(T)} if {@link #getPrefixes(T)} is empty;</li>
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
