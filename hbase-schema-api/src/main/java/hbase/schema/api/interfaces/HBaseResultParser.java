package hbase.schema.api.interfaces;

import java.util.NavigableMap;

/**
 * Interface to parse raw data from HBase into Java objects
 *
 * @param <T> result object instance
 */
public interface HBaseResultParser<T> {
    /**
     * Creates a new instance of the result object
     * <p>
     * The implementation is likely to be something like {@code return new SomePojo(); }
     */
    T newInstance();

    /**
     * Populates the object with data from the row key
     * <p>
     * The default implementation does nothing
     *
     * @param obj    result object instance
     * @param rowKey row key bytes
     */
    default void setFromRowKey(T obj, byte[] rowKey) {
        // nothing to do by default
    }

    /**
     * Populates the object with data from HBase
     *
     * @param obj         result object instance
     * @param resultCells cells fetched from HBase, in a mapping (qualifier -> cell value)
     */
    void setFromResult(T obj, NavigableMap<byte[], byte[]> resultCells);
}
