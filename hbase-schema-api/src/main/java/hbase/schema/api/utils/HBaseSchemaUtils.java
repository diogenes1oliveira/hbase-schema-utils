package hbase.schema.api.utils;

import hbase.base.interfaces.TriConsumer;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Miscellaneous utilities for schemas
 */
@SuppressWarnings("java:S1319")
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

    /**
     * Builds a new map keyed by binary bytes
     *
     * @return map sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static <T> TreeMap<byte[], T> asBytesTreeMap() {
        return new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

    /**
     * Builds a new map keyed by binary bytes
     *
     * @param key   initial key
     * @param value initial value
     * @param <T>   value type
     * @return map sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     */
    public static <T> TreeMap<byte[], T> asBytesTreeMap(byte[] key, T value) {
        TreeMap<byte[], T> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        map.put(key, value);
        return map;
    }

    /**
     * Builds a new map keyed by binary bytes
     *
     * @param firstKey           initial key
     * @param firstValue         initial value
     * @param otherKeysAndValues array [key1, value1, key2, value2, ...]
     * @param <T>                value type
     * @return map sorted by bytes values using {@link Bytes#BYTES_COMPARATOR}
     * @throws IllegalArgumentException key without corresponding value in {@code otherKeysAndValues}
     * @throws ClassCastException       value with type not compatible with {@code firstValue}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> TreeMap<byte[], T> asBytesTreeMap(byte[] firstKey,
                                                        @NotNull T firstValue,
                                                        Object... otherKeysAndValues) {
        if (otherKeysAndValues.length % 2 != 0) {
            int lastKeyIndex = (otherKeysAndValues.length - 1) / 2;
            throw new IllegalArgumentException("Key #" + lastKeyIndex + " doesn't have a value");
        }
        Class<T> valueClass = (Class) firstValue.getClass();
        TreeMap<byte[], T> map = asBytesTreeMap(firstKey, firstValue);

        for (int i = 0; i < otherKeysAndValues.length; i += 2) {
            byte[] key = (byte[]) otherKeysAndValues[i];
            Object value = otherKeysAndValues[i + 1];
            if (value != null && !valueClass.isAssignableFrom(value.getClass())) {
                throw new ClassCastException("Invalid value type");
            }
            map.put(key, (T) value);
        }

        return map;
    }

    /**
     * Creates a string map from an array of keys and values (similar to {@code Map.of()} in Java 9)
     *
     * @param keysAndValues array [key1, value1, key2, value2, ...]
     * @return map of (key -> value)
     * @throws IllegalArgumentException key without corresponding value
     */
    public static LinkedHashMap<String, String> asStringMap(String... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            int lastKeyIndex = (keysAndValues.length - 1) / 2;
            throw new IllegalArgumentException("Key #" + lastKeyIndex + " doesn't have a value");
        }

        LinkedHashMap<String, String> map = new LinkedHashMap<>();

        for (int i = 0; i < keysAndValues.length; i += 2) {
            String key = keysAndValues[i];
            String value = keysAndValues[i + 1];
            map.put(key, value);
        }

        return map;
    }

    /**
     * Checks if at least one object is non-null
     *
     * @param message message for the exception
     * @param objs    objects to be verified
     * @throws IllegalStateException couldn't find a non-null object
     */
    public static void verifyNonNull(String message, Object... objs) {
        for (Object obj : objs) {
            if (obj != null) {
                return;
            }
        }
        throw new IllegalStateException(message);
    }

    /**
     * Checks if at least one collection is non-empty
     *
     * @param message     message for the exception
     * @param collections collections to be verified
     * @throws IllegalStateException couldn't find a non-empty collection
     */
    public static void verifyNonEmpty(String message, Collection<?>... collections) {
        for (Collection<?> collection : collections) {
            if (collection != null && !collection.isEmpty()) {
                return;
            }
        }
        throw new IllegalStateException(message);
    }

    /**
     * Creates a new array by applying a mapper the items in the input
     *
     * @param input      input array
     * @param outputType type of output items
     * @param mapper     maps an input item
     * @param <T>        input type
     * @param <U>        output type
     * @return new array with the mapped items
     */
    @SuppressWarnings("unchecked")
    public static <T, U> U[] mapArray(T[] input, Class<U> outputType, Function<T, U> mapper) {
        U[] output = (U[]) Array.newInstance(outputType, input.length);
        for (int i = 0; i < input.length; ++i) {
            output[i] = mapper.apply(input[i]);
        }
        return output;
    }

    /**
     * Builds a new function that applies the functions in sequence, handling null values accordingly.
     *
     * @param f1  first function in chain
     * @param f2  second function in chain
     * @param <T> first function input type
     * @param <U> first function output type and second function input type
     * @param <V> second function output type
     * @return new function equivalent to {@code f2(f1(t))}
     */
    public static <T, U, V> Function<T, V> chain(Function<T, U> f1, Function<U, V> f2) {
        return t -> {
            if (t == null) {
                return null;
            }
            U u = f1.apply(t);
            return u != null ? f2.apply(u) : null;
        };
    }

    /**
     * Builds a new bi-consumer that applies a transformer function beforehand, handling null values accordingly.
     *
     * @param c1  bi-consumer
     * @param f2  function
     * @param <T> bi-consumer first input type
     * @param <U> bi-consumer second input type and function output type
     * @param <V> function input type
     * @return new bi-consumer equivalent to {@code c1(t, f2(v))}
     */
    public static <T, U, V> BiConsumer<T, V> chain(BiConsumer<T, U> c1, Function<V, U> f2) {
        return (t, v) -> {
            if (v == null) {
                return;
            }
            U u = f2.apply(v);
            if (u != null) {
                c1.accept(t, u);
            }
        };
    }

    /**
     * Builds a new bi-consumer that applies a transformer function beforehand, handling null values accordingly.
     *
     * @param c tri-consumer
     * @return new bi-consumer equivalent to {@code }
     */
    public static <T, U1, V1, U2, V2> TriConsumer<T, U2, V2> chain(TriConsumer<T, U1, V1> c, Function<U2, U1> fU, Function<V2, V1> fV) {
        return (t, u2, v2) -> {
            if (u2 == null || v2 == null) {
                return;
            }
            U1 u1 = fU.apply(u2);
            V1 v1 = fV.apply(v2);
            if (u1 == null || v1 == null) {
                return;
            }
            c.accept(t, u1, v1);
        };
    }

    /**
     * Builds a new function that applies the functions in sequence, handling null and empty values accordingly.
     *
     * @param f1  first function in chain
     * @param f2  second function in chain
     * @param <L> first function input type
     * @param <U> first function output list type and second function input type
     * @param <V> second function output type
     * @return new function equivalent to {@code f2(f1(t))}
     */
    @SuppressWarnings("ConstantConditions")
    public static <L, U, V> Function<L, List<V>> chainMap(Function<L, List<U>> f1, Function<U, V> f2) {
        return l -> {
            if (l == null) {
                return emptyList();
            }
            List<U> us = f1.apply(l);
            if (l == null) {
                return emptyList();
            }
            return us.stream().filter(Objects::nonNull).map(f2).filter(Objects::nonNull).collect(toList());
        };
    }

    /**
     * Combines the filters, skipping the null ones
     *
     * @param operator operator to combine the filters into a {@link FilterList}
     * @param filters  iterator that yields the (potentially null) filters to be combined
     * @return a FilterList, the only non-null Filter or null if no valid filter was found
     */
    @Nullable
    public static Filter combineNullableFilters(FilterList.Operator operator, Iterator<@Nullable Filter> filters) {
        FilterList filterList = new FilterList(operator);
        while (filters.hasNext()) {
            Filter filter = filters.next();
            if (filter != null) {
                filterList.addFilter(filter);
            }
        }

        switch (filterList.size()) {
            case 0:
                return null;
            case 1:
                return filterList.getFilters().get(0);
            default:
                return filterList;
        }
    }

    /**
     * Combines the filters, skipping the null ones
     *
     * @param operator operator to combine the filters into a {@link FilterList}
     * @param filters  array of filters to be combined
     * @return a FilterList, the only non-null Filter or null if no valid filter was found
     */
    @Nullable
    public static Filter combineNullableFilters(FilterList.Operator operator, @Nullable Filter... filters) {
        return combineNullableFilters(operator, asList(filters).iterator());
    }

    /**
     * A printable representation of a collection of {@code byte[]} values
     */
    public static String bytesCollectionToString(Collection<byte[]> bytesCollection) {
        return "[" + bytesCollection.stream()
                                    .map(Bytes::toStringBinary)
                                    .collect(joining(", ")) + "]";
    }

    /**
     * A dummy {@link BiConsumer} that doesn't do anything
     */
    public static <T, U> BiConsumer<T, U> dummyBiConsumer() {
        return (t, u) -> {
            // do nothing
        };
    }
}
