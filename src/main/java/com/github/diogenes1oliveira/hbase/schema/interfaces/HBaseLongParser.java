package com.github.diogenes1oliveira.hbase.schema.interfaces;

import static com.github.diogenes1oliveira.hbase.schema.utils.HBaseUtils.bytesToLong;

/**
 * Interface to parse data from a long value into a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseLongParser<T> extends HBaseBytesParser<T> {
    /**
     * @param pojo  POJO instance to set the fields of
     * @param value long data from cell value or timestamp
     */
    void setFromLong(T pojo, long value);

    /**
     * Default implementation that parses the binary payload to a long value and calls
     * {@link #setFromLong(T, long)}
     *
     * @param pojo  POJO instance to set the fields of
     * @param bytes array with 8 bytes to be converted into a
     */
    @Override
    default void setFromBytes(T pojo, byte[] bytes) {
        long value = bytesToLong(bytes);
        setFromLong(pojo, value);
    }
}
