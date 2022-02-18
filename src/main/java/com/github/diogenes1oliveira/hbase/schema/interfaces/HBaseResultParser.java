package com.github.diogenes1oliveira.hbase.schema.interfaces;

import java.util.SortedMap;

/**
 * Interface to populate a POJO with data fetched from HBase
 *
 * @param <T> POJO type
 */
public interface HBaseResultParser<T> {
    /**
     * Populates the POJO with data from the row key, qualifiers and values fetched from HBase
     * <p>
     * This default implementation forwards to {@link #parseRowKey(T, byte[])} and
     * {@link #parseValues(T, SortedMap)}. You shouldn't need to override this method.
     *
     * @param pojo   POJO object
     * @param rowKey row key binary data
     */
    default void parseResult(T pojo, byte[] rowKey, SortedMap<byte[], byte[]> valuesByQualifier) {
        parseRowKey(pojo, rowKey);
        parseValues(pojo, valuesByQualifier);
    }

    /**
     * Populates the POJO with data from HBase cells
     *
     * @param pojo              POJO object
     * @param valuesByQualifier map of (qualifier -> value)
     */
    void parseValues(T pojo, SortedMap<byte[], byte[]> valuesByQualifier);

    /**
     * Populates the POJO with data from the row key
     * <p>
     * The default implementation does nothing. Override this if you do need to parse some data
     * from the row key itself
     *
     * @param pojo   POJO object
     * @param rowKey row key binary data
     */
    default void parseRowKey(T pojo, byte[] rowKey) {
        // nothing to do by default
    }


}
