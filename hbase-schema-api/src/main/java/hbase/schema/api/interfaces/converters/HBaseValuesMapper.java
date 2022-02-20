package hbase.schema.api.interfaces.converters;

import java.util.SortedMap;

/**
 * Interface to generate HBase cell data from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseValuesMapper<T> {
    /**
     * Generates the cells data based on the POJO fields
     *
     * @param pojo POJO object
     * @return sorted map (qualifier -> cell value)
     */
    SortedMap<byte[], byte[]> getBytes(T pojo);
}
