package hbase.schema.api.utils;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSortedMap;
import static java.util.Collections.unmodifiableSortedSet;

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
    public static SortedSet<byte[]> frozenSortedByteSet(byte[]... values) {
        SortedSet<byte[]> set = sortedByteSet();
        set.addAll(asList(values));
        return unmodifiableSortedSet(set);
    }

    @SuppressWarnings("unchecked")
    private static <T> SortedMap<byte[], T> frozenSortedByteMapGeneric(Object... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            throw new IllegalStateException("Key without value");
        }
        SortedMap<byte[], T> map = sortedByteMap();

        for (int i = 0; i < keysAndValues.length; i += 2) {
            byte[] key = (byte[]) keysAndValues[i];
            T value = (T) keysAndValues[i + 1];
            map.put(key, value);
        }
        return unmodifiableSortedMap(map);
    }

    public static <T> SortedMap<byte[], T> frozenSortedByteMap() {
        return frozenSortedByteMapGeneric();
    }

    public static SortedMap<byte[], byte[]> frozenSortedByteMap(byte[] key, byte[] value) {
        return frozenSortedByteMapGeneric(key, value);
    }

    public static SortedMap<byte[], Long> frozenSortedByteMap(byte[] key, Long value) {
        return frozenSortedByteMapGeneric(key, value);
    }

    @SuppressWarnings("unchecked")
    public static <T> T verifiedCast(Class<T> type, Object value) {
        if (value == null) {
            return null;
        } else if (!type.isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Invalid object type");
        }
        return (T) value;
    }


    @Nullable
    public static byte[] findCommonPrefix(Collection<byte[]> bytesCollection) {
        List<Byte> common = new ArrayList<>();

        for (int i = 0; ; i++) {
            byte current = 0;
            boolean first = true;
            boolean different = false;
            for (byte[] bytes : bytesCollection) {
                if (bytes.length <= i) {
                    break;
                }
                byte b = bytes[i];
                if (first) {
                    current = b;
                    first = false;
                } else if (b != current) {
                    different = true;
                    break;
                }
            }
            if (!different) {
                common.add(current);
            } else {
                break;
            }
        }

        if (common.isEmpty()) {
            return null;
        } else {
            byte[] prefix = new byte[common.size()];
            for (int i = 0; i < common.size(); ++i) {
                prefix[i] = common.get(i);
            }
            return prefix;
        }
    }

}
