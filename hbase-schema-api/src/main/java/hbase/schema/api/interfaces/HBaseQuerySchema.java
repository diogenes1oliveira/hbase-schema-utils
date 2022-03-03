package hbase.schema.api.interfaces;

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
     * Generates the search key to be used in a Scan request
     * <p>
     * The default implementation just delegates to {@link #buildRowKey(T)}
     *
     * @param query input query object
     * @return this builder
     */
    default byte[] buildScanKey(T query) {
        return buildRowKey(query);
    }

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
}
