package hbase.schema.api.interfaces;

import java.util.NavigableMap;

@FunctionalInterface
public interface HBaseBytesMapSetter<T> {
    void setFromBytes(T obj, NavigableMap<byte[], byte[]> bytesMap);


    static <T> HBaseBytesMapSetter<T> dummy() {
        return ((obj, bytesMap) -> {

        });
    }
}
