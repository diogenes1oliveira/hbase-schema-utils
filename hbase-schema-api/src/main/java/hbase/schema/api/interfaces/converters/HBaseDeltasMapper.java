package hbase.schema.api.interfaces.converters;

import java.util.SortedMap;

/**
 * Interface to generate HBase increment delta values from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseDeltasMapper<T> {
    /**
     * Generates the increment delta values based on the POJO fields
     *
     * @param pojo POJO object
     * @return sorted map (qualifier -> increment delta value)
     */
    SortedMap<byte[], Long> getDeltas(T pojo);
}
