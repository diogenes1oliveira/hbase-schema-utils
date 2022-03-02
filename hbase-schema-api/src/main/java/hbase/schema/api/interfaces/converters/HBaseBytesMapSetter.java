package hbase.schema.api.interfaces.converters;

import java.util.NavigableMap;

@FunctionalInterface
public interface HBaseBytesMapSetter<T> {
    void setFromBytes(T obj, NavigableMap<byte[], byte[]> bytesMap);
}
