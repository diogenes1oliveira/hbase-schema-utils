package dev.diogenes.hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Result;

public interface ResultParser<T> {
    default boolean parseRowKey(T obj, ByteBuffer rowKey) {
        // nothing to parse by default
        return false;
    }

    default boolean parseCell(T obj, ByteBuffer column, ByteBuffer value) {
        // nothing to parse by default
        return false;
    }

    T newInstance();

    default Optional<T> parseCells(T obj, NavigableMap<byte[], byte[]> cellsMap) {
        boolean parsed = false;

        if (cellsMap != null) {
            for (Map.Entry<byte[], byte[]> entry : cellsMap.entrySet()) {
                byte[] column = entry.getKey();
                byte[] value = entry.getValue();
                if (value != null) {
                    parsed = parseCell(obj, ByteBuffer.wrap(column), ByteBuffer.wrap(value)) || parsed;
                }
            }
        }

        if (!parsed) {
            return Optional.empty();
        }

        return Optional.of(obj);
    }

}
