package hbase.schema.api.interfaces.converters;

import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Interface to extract a map ({@code byte[]} -> {@code Long}) from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseLongMapGetter<T> {
    NavigableMap<byte[], Long> getLongMap(T obj);

    /**
     * Builds a map getter that always returns an empty map
     *
     * @param <T> object type
     * @return map getter instance
     */
    static <T> HBaseLongMapGetter<T> empty() {
        NavigableMap<byte[], Long> result = asBytesTreeMap();
        return obj -> result;
    }
}
