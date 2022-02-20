package hbase.schema.api.interfaces.converters;

import java.util.SortedMap;

/**
 * Interface to generate HBase 8-bytes Long values from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseLongsMapper<T> {
    /**
     * Generates the long values based on the POJO fields
     *
     * @param pojo POJO object
     * @return sorted map (qualifier -> increment delta value)
     */
    SortedMap<byte[], Long> getLongs(T pojo);
}
