package hbase.schema.api.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.Arrays.asList;

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
     * Builds a new set keyed by binary bytes,
     *
     * @return set sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static TreeSet<byte[]> asBytesTreeSet(byte[]... values) {
        TreeSet<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        set.addAll(asList(values));
        return set;
    }

    public static <T> TreeMap<byte[], T> asBytesTreeMap() {
        return new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

    public static <T> TreeMap<byte[], T> asBytesTreeMap(byte[] key, T value) {
        TreeMap<byte[], T> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        map.put(key, value);
        return map;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> TreeMap<byte[], T> asBytesTreeMap(byte[] firstKey, @NonNull T firstValue, Object... otherKeysAndValues) {
        Class<T> valueClass = (Class) firstValue.getClass();
        return asBytesTreeMap(valueClass, firstKey, firstValue, otherKeysAndValues);
    }

    @SuppressWarnings("unchecked")
    public static <T> TreeMap<byte[], T> asBytesTreeMap(Class<T> valueClass,
                                                        byte[] firstKey,
                                                        T firstValue,
                                                        Object... otherKeysAndValues) {
        if (otherKeysAndValues.length % 2 != 0) {
            int lastKeyIndex = (otherKeysAndValues.length - 1) / 2;
            throw new IllegalArgumentException("Key #" + lastKeyIndex + " doesn't have a value");
        }
        TreeMap<byte[], T> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        map.put(firstKey, firstValue);

        for (int i = 0; i < otherKeysAndValues.length; i += 2) {
            byte[] key = (byte[]) otherKeysAndValues[i];
            Object value = otherKeysAndValues[i + 1];
            if (value != null && !valueClass.isAssignableFrom(value.getClass())) {
                throw new IllegalArgumentException("Invalid value type");
            }
            map.put(key, (T) value);
        }

        return map;
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
