package hbase.schema.api.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

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

    public static MultiRowRangeFilter toMultiRowRangeFilter(Iterator<byte[]> it) {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        while (it.hasNext()) {
            byte[] prefixStart = it.next();
            if (prefixStart == null) {
                throw new IllegalArgumentException("No search key generated for query");
            }
            byte[] prefixStop = Bytes.incrementBytes(prefixStart, 1);
            MultiRowRangeFilter.RowRange range = new MultiRowRangeFilter.RowRange(prefixStart, true, prefixStop, false);
            ranges.add(range);
        }

        return new MultiRowRangeFilter(ranges);
    }

    public static void selectColumns(Scan scan,
                                     byte[] family,
                                     SortedSet<byte[]> qualifiers,
                                     SortedSet<byte[]> prefixes) {
        if (prefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                scan.addColumn(family, qualifier);
            }
        } else {
            scan.addFamily(family);
        }
    }

    public static void selectColumns(Get get,
                                     byte[] family,
                                     SortedSet<byte[]> qualifiers,
                                     SortedSet<byte[]> prefixes) {
        if (prefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                get.addColumn(family, qualifier);
            }
        } else {
            get.addFamily(family);
        }
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

    public static <O, F> Function<O, byte[]> bytesGetter(Function<O, F> getter, Function<F, byte[]> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }

    public static <O> Function<O, byte[]> stringGetter(Function<O, String> getter) {
        return obj -> {
            String value = getter.apply(obj);
            return value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        };
    }

    public static <O, F> Function<O, byte[]> stringGetter(Function<O, F> getter, Function<F, String> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value).getBytes(StandardCharsets.UTF_8) : null;
        };
    }


    public static <O> Function<O, byte[]> jsonGetter(ObjectMapper mapper) {
        return obj -> {
            try {
                return mapper.writeValueAsBytes(obj);
            } catch (IOException e) {
                throw new IllegalArgumentException("Couldn't deserialize the payload from JSON", e);
            }
        };
    }

    public static <O, F> Function<O, Long> longGetter(Function<O, F> getter, Function<F, Long> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }

    public static <O> Function<O, Long> booleanGetter(Function<O, Boolean> getter) {
        return obj -> {
            Boolean value = getter.apply(obj);
            if (value == null) {
                return null;
            }
            return value ? 1L : 0L;
        };
    }

    public static <T, I> Function<T, NavigableMap<byte[], byte[]>> listColumnGetter(
            Function<T, List<? extends I>> listGetter,
            Function<I, byte[]> qualifierConverter,
            Function<I, byte[]> valueConverter
    ) {
        return obj -> {
            NavigableMap<byte[], byte[]> cellsMap = asBytesTreeMap();
            List<? extends I> list = listGetter.apply(obj);
            if (list == null || list.isEmpty()) {
                return cellsMap;
            }
            for (I item : list) {
                byte[] value = valueConverter.apply(item);
                byte[] qualifier = qualifierConverter.apply(item);
                if (value != null && qualifier != null) {
                    cellsMap.put(qualifier, value);
                }
            }
            return cellsMap;
        };
    }

    public static <O> BiConsumer<O, byte[]> stringSetter(BiConsumer<O, String> setter) {
        return (obj, bytes) -> {
            String value = new String(bytes, StandardCharsets.UTF_8);
            setter.accept(obj, value);
        };
    }

    public static <O, F> BiConsumer<O, byte[]> stringSetter(BiConsumer<O, F> setter, Function<String, F> converter) {
        return (obj, bytes) -> {
            String value = new String(bytes, StandardCharsets.UTF_8);
            setter.accept(obj, converter.apply(value));
        };
    }

    public static <O> BiConsumer<O, byte[]> jsonSetter(ObjectMapper mapper) {
        return (obj, bytes) -> {
            try {
                mapper.readerForUpdating(obj).readValue(bytes);
            } catch (IOException e) {
                throw new IllegalArgumentException("Couldn't deserialize the payload from JSON", e);
            }
        };
    }

    public static <O> BiConsumer<O, byte[]> longSetter(BiConsumer<O, Long> setter) {
        return (obj, bytes) -> {
            Long value = Bytes.toLong(bytes);
            setter.accept(obj, value);
        };
    }

    public static <O, F> BiConsumer<O, byte[]> longSetter(BiConsumer<O, F> setter, Function<Long, F> converter) {
        return (obj, bytes) -> {
            Long value = Bytes.toLong(bytes);
            setter.accept(obj, converter.apply(value));
        };
    }
}
