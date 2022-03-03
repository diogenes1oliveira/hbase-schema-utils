package hbase.schema.api.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.Arrays.asList;

public final class HBaseSchemaUtils {
    private HBaseSchemaUtils() {
        // utility class
    }

    /**
     * Builds a new set keyed by binary bytes
     *
     * @param values array of values to be added to the set
     * @return set sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static TreeSet<byte[]> asBytesTreeSet(byte[]... values) {
        return asBytesTreeSet(asList(values));
    }

    /**
     * Builds a new set keyed by binary bytes
     *
     * @param values collection of values to be added to the set
     * @return set sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static TreeSet<byte[]> asBytesTreeSet(Collection<byte[]> values) {
        TreeSet<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        set.addAll(values);
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
    public static <T> TreeMap<byte[], T> asBytesTreeMap(byte[] firstKey, @NotNull T firstValue, Object... otherKeysAndValues) {
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

    public static void verifyNonNull(String message, Object... objs) {
        for (Object obj : objs) {
            if (obj != null) {
                return;
            }
        }
        if (objs.length > 0) {
            throw new IllegalStateException(message);
        }
    }

    public static void verifyNonEmpty(String message, Collection<?>... collections) {
        for (Collection<?> collection : collections) {
            if (collection != null && !collection.isEmpty()) {
                return;
            }
        }
        if (collections.length > 0) {
            throw new IllegalStateException(message);
        }
    }
}
