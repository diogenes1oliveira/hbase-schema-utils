package hbase.schema.api.interfaces.converters;

import java.util.NavigableMap;

public interface HBaseBytesMapGetter<T> {
    NavigableMap<byte[], byte[]> getBytesMap(T obj);
}
