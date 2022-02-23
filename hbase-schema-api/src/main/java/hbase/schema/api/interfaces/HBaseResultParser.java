package hbase.schema.api.interfaces;

import java.util.NavigableMap;

public interface HBaseResultParser<T> {

    T newInstance();

    default void setFromRowKey(T obj, byte[] rowKey) {
        // nothing to do by default
    }

    void setFromResult(T obj, NavigableMap<byte[], byte[]> cellsMap);

    default void setFromCell(T obj, byte[] qualifier, byte[] value) {
        // nothing to do by default
    }

    default void setFromPrefix(T obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsMap) {
        // nothing to do by default
    }
}
