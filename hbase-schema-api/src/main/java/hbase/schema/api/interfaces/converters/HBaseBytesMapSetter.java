package hbase.schema.api.interfaces.converters;

import java.util.NavigableMap;

/**
 * Interface to populate a Java object with data parsed from a map ({@code byte[]} -> {@code byte[]})
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseBytesMapSetter<T> {
    void setFromBytes(T obj, NavigableMap<byte[], byte[]> bytesMap);
}
