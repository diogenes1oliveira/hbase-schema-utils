package hbase.schema.api.utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * Generic utility aid methods
 */
public final class HBaseSchemaUtils {
    private HBaseSchemaUtils() {
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
    public static <T> SortedMap<byte[], T> sortedByteMap() {
        return new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

    /**
     * Builds a new set keyed by binary bytes
     *
     * @return set sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static SortedSet<byte[]> sortedByteSet() {
        return new TreeSet<>(Bytes.BYTES_COMPARATOR);
    }

    /**
     * Builds a new set keyed by binary bytes,
     *
     * @return set sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static Set<byte[]> frozenSortedByteSet(byte[]... values) {
        SortedSet<byte[]> set = sortedByteSet();
        set.addAll(asList(values));
        return unmodifiableSet(set);
    }

}
