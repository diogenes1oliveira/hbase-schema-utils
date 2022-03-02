package hbase.schema.api.interfaces.converters;

import java.util.NavigableMap;

public interface HBaseLongMapGetter<T> {
    NavigableMap<byte[], Long> getLongMap(T obj);
}
