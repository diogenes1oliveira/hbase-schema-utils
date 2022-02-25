package hbase.schema.api.interfaces;

import java.util.NavigableMap;

@FunctionalInterface
public interface HBaseFromBytesMapSetter<T> {
    void setFromBytes(T obj, NavigableMap<byte[], byte[]> bytesMap);


    static <T> HBaseFromBytesMapSetter<T> dummy() {
        return ((obj, bytesMap) -> {

        });
    }
}
