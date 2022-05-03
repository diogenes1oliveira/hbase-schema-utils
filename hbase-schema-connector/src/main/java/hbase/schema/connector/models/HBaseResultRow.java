package hbase.schema.connector.models;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseResultRow implements Comparable<HBaseResultRow> {
    private final byte[] rowKey;
    private final NavigableMap<byte[], byte[]> cellsMap;

    public HBaseResultRow(byte[] rowKey, NavigableMap<byte[], byte[]> cellsMap) {
        this.rowKey = requireNonNull(rowKey, "row key can't be null");
        this.cellsMap = requireNonNull(cellsMap, "cells map can't be null");
    }

    public ByteBuffer rowKey() {
        return ByteBuffer.wrap(rowKey).asReadOnlyBuffer();
    }

    public NavigableMap<byte[], byte[]> cellsMap() {
        return cellsMap;
    }

    public int columnCount() {
        return cellsMap.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HBaseResultRow)) {
            return false;
        }
        HBaseResultRow other = (HBaseResultRow) o;
        return Arrays.equals(this.rowKey, other.rowKey);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rowKey);
    }

    @Override
    public int compareTo(@NotNull HBaseResultRow other) {
        return Bytes.BYTES_COMPARATOR.compare(this.rowKey, other.rowKey);
    }

    @Override
    public String toString() {
        return "HBaseResultRow{" +
                "rowKey=" + toStringBinary(rowKey) +
                ", cellsMap=" + toStringBinaryMap(cellsMap) +
                '}';
    }

    public static HBaseResultRow fromResult(byte[] family, Result result) {
        return new HBaseResultRow(result.getRow(), result.getFamilyMap(family));
    }

    public static List<HBaseResultRow> fromResults(byte[] family, Result... results) {
        return Arrays.stream(results)
                     .map(result -> fromResult(family, result))
                     .collect(toList());
    }

    private static Map<String, String> toStringBinaryMap(NavigableMap<byte[], byte[]> cells) {
        LinkedHashMap<String, String> printableMap = new LinkedHashMap<>();

        for (Map.Entry<byte[], byte[]> entry : cells.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();

            printableMap.put(toStringBinary(key), toStringBinary(value));
        }

        return printableMap;
    }
}
