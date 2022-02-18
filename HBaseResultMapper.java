package com.github.diogenes1oliveira.hbase.schema;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Optional;

import static com.github.diogenes1oliveira.hbase.utils.PayloadUtils.getQualifier;
import static com.github.diogenes1oliveira.hbase.utils.PayloadUtils.getValue;
import static java.util.Optional.ofNullable;

public interface HBaseResultMapper<T> {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    Optional<HBaseBytesReader<T>> valueReader(byte[] family, Optional<byte[]> qualifier);

    default void parseResult(byte[] family, Result result, T obj) throws IOException {
        byte[] rowKey = result.getRow();
        if (rowKey == null) {
            return;
        }
        parseRowKey(family, rowKey, obj);
        parseCells(family, result, obj);
    }

    default void parseRowKey(byte[] family, byte[] rowKey, T obj) {
        // nothing to do by default
    }

    default void parseCells(byte[] family, Result result, T obj) throws IOException {
        CellScanner scanner = result.cellScanner();
        if (scanner == null) {
            return;
        }
        Long timestamp = null;

        while (scanner.advance()) {
            Cell cell = scanner.current();
            Optional<byte[]> qualifier = ofNullable(getQualifier(cell));
            byte[] value = getValue(cell);
            timestamp = cell.getTimestamp();

            valueReader(family, qualifier)
                    .ifPresent(reader -> reader.parseBytes(value, obj));
        }

        parseTimestamp(family, result.getRow(), timestamp, obj);
    }

    default void parseTimestamp(byte[] family, byte[] rowKey, Long timestamp, T obj) {
        // nothing to by default
    }

}
