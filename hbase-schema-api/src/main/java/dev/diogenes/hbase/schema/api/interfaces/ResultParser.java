package dev.diogenes.hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Supplier;

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

    default boolean parseCells(T obj, NavigableMap<byte[], byte[]> cellsMap) {
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

        return parsed;
    }

    static <T> ResultParser<T> combineParsers(List<? extends ResultParser<? super T>> parsers, Supplier<T> newInstance) {

        return new ResultParser<T>() {
            @Override
            public T newInstance() {
                return newInstance.get();
            }

            @Override
            public boolean parseRowKey(T obj, ByteBuffer rowKey) {
                for (ResultParser<? super T> parser : parsers) {
                    if (parser.parseRowKey(obj, rowKey)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean parseCell(T obj, ByteBuffer column, ByteBuffer value) {
                for (ResultParser<? super T> parser : parsers) {
                    if (parser.parseCell(obj, column, value)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean parseCells(T obj, NavigableMap<byte[], byte[]> cellsMap) {
                for (ResultParser<? super T> parser : parsers) {
                    if (parser.parseCells(obj, cellsMap)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

}
