package com.github.diogenes1oliveira.hbase.schema.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Generic utility aid methods
 */
public final class HBaseUtils {
    private HBaseUtils() {
        // utility class
    }

    /**
     * Converts the binary value array into a Long instance
     *
     * @param value binary value
     * @return converted long value
     * @throws IllegalArgumentException value null or not with 8 bytes
     */
    public static long bytesToLong(byte[] value) {
        if (value == null || value.length != 8) {
            throw new IllegalArgumentException("Invalid Long value");
        }
        return Bytes.toLong(value);
    }

    /**
     * Builds a new map keyed by binary bytes
     *
     * @param <T> value type
     * @return map sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static <T> SortedMap<byte[], T> comparableByteMap() {
        return new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

    /**
     * Extracts the column qualifier from the cell
     *
     * @param cell HBase result cell
     * @return copied qualifier array or null
     */
    public static byte[] getCellQualifier(Cell cell) {
        if (cell.getQualifierArray() == null) {
            return null;
        }
        return Arrays.copyOfRange(
                cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierOffset() + cell.getQualifierLength()
        );
    }

    /**
     * Extracts the binary value from the cell
     *
     * @param cell HBase result cell
     * @return copied value array or null
     */
    public static byte[] getCellValue(Cell cell) {
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
