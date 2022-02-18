package com.github.diogenes1oliveira.hbase.schema.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Interface to extract binary data from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseBytesExtractor<T> {
    /**
     * @param obj POJO to get data from
     * @return binary data based on the POJO fields
     */
    @Nullable
    byte[] getBytes(T obj);
}
