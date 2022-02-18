package com.github.diogenes1oliveira.hbase.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public final class PayloadUtils {
    private PayloadUtils() {
        // utility class
    }

    public static Map<byte[], byte[]> prefixSubMap(Map<byte[], byte[]> map, byte[] prefix) {
        TreeMap<byte[], byte[]> subMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (Bytes.startsWith(entry.getKey(), prefix)) {
                subMap.put(entry.getKey(), entry.getValue());
            }
        }

        return subMap;
    }

    public static long bytesToLong(byte[] value) {
        if (value == null || value.length != 8) {
            throw new IllegalArgumentException("Invalid Long value");
        }
        return Bytes.toLong(value);
    }

    public static <T> Map<byte[], T> singletonByteMap(byte[] key, T value) {
        TreeMap<byte[], T> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        map.put(key, value);
        return map;
    }

    public static <T> Map<byte[], T> emptyByteMap() {
        return new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

    public static byte[] getQualifier(Cell cell) {
        if (cell.getQualifierArray() == null) {
            return null;
        }
        return Arrays.copyOfRange(
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierOffset() + cell.getQualifierLength()
        );
    }

    public static byte[] getValue(Cell cell) {
        if (cell.getValueArray() == null) {
            return null;
        }
        return Arrays.copyOfRange(
                cell.getValueArray(),
                cell.getValueOffset(),
                cell.getValueOffset() + cell.getValueLength()
        );
    }
}
