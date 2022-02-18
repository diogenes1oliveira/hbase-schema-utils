package com.github.diogenes1oliveira.hbase.schema.interfaces;

/**
 * Interface to generate HBase cell attributes from a POJO object
 *
 * @param <T> POJO type
 */
public interface HBaseAttributesMapper<T> {
    /**
     * Generates a row key based on the POJO fields
     *
     * @param pojo POJO object
     * @return row key bytes
     */
    byte[] getRowKey(T pojo);

    /**
     * Generates a timestamp based on the POJO fields
     *
     * @param pojo POJO object
     * @return timestamp in milliseconds
     */
    long getTimestamp(T pojo);
}
