package hbase.schema.api.interfaces.converters;

import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Interface to extract a map ({@code byte[]} -> {@code byte[]}) from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseBytesMapGetter<T> {
    NavigableMap<byte[], byte[]> getBytesMap(T obj);

    /**
     * Builds a map getter that always returns an empty map
     *
     * @param <T> object type
     * @return map getter instance
     */
    static <T> HBaseBytesMapGetter<T> empty() {
        NavigableMap<byte[], byte[]> result = asBytesTreeMap();
        return obj -> result;
    }
}
