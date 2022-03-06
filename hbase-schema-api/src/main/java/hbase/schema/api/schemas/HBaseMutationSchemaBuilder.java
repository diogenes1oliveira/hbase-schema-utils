package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.interfaces.conversion.BytesMapConverter;
import hbase.schema.api.interfaces.conversion.LongConverter;
import hbase.schema.api.interfaces.conversion.LongMapConverter;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonEmpty;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonNull;

/**
 * Builder for {@link HBaseMutationSchema} objects, providing a fluent API to add fields and field prefixes to be inserted
 *
 * @param <T> result object instance
 */
@SuppressWarnings("java:S4276")
public class HBaseMutationSchemaBuilder<T> {
    private static final byte[] EMPTY = new byte[0];
    private final List<Function<T, NavigableMap<byte[], byte[]>>> valueBuilders = new ArrayList<>();
    private final List<Function<T, NavigableMap<byte[], Long>>> deltaBuilders = new ArrayList<>();
    private final NavigableMap<byte[], Function<T, Long>> timestampBuilders = asBytesTreeMap();
    private Function<T, byte[]> rowKeyBuilder = null;
    private Function<T, Long> timestampBuilder = null;
    private Function<T, Long> currentTimestampBuilder = null;

    /**
     * Sets up the row key generation
     *
     * @param getter lambda to get the row key bytes from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withRowKey(Function<T, byte[]> getter) {
        if (currentTimestampBuilder == null) {
            throw new IllegalStateException("You need to set a timestamp for the row key");
        }
        this.timestampBuilder = currentTimestampBuilder;
        this.rowKeyBuilder = getter;
        return this;
    }

    /**
     * Sets up the row key generation
     *
     * @param getter    lambda to get a row key value from the object
     * @param converter converts the value into a proper {@code byte[]}
     * @param <U>       row key value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withRowKey(Function<T, U> getter, Function<U, byte[]> converter) {
        return withRowKey(chain(getter, converter));
    }

    /**
     * Sets up the row key generation
     *
     * @param getter    lambda to get a row key value from the object
     * @param converter converts the value into a proper {@code byte[]}
     * @param <U>       row key value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withRowKey(Function<T, U> getter, BytesConverter<U> converter) {
        return withRowKey(getter, converter::toBytes);
    }

    /**
     * Sets up the timestamp generator
     *
     * @param getter lambda to get the cell timestamp as a milliseconds value
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withTimestamp(Function<T, Long> getter) {
        this.currentTimestampBuilder = getter;
        return this;
    }

    /**
     * Sets up the timestamp generator
     *
     * @param getter    lambda to get the cell timestamp value
     * @param converter converts the value into a proper milliseconds {@code Long}
     * @param <U>       timestamp value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withTimestamp(Function<T, U> getter, Function<U, Long> converter) {
        return withTimestamp(chain(getter, converter));
    }

    /**
     * Sets up the timestamp generator
     *
     * @param getter    lambda to get the cell timestamp value
     * @param converter converts the value into a proper milliseconds {@code Long}
     * @param <U>       timestamp value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withTimestamp(Function<T, U> getter, LongConverter<U> converter) {
        return withTimestamp(getter, converter::toLong);
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix qualifier prefix bytes
     * @param getter lambda to extract a map of (qualifier suffix -> cell value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValues(byte[] prefix, Function<T, NavigableMap<byte[], byte[]>> getter) {
        timestampBuilders.put(prefix, currentTimestampBuilder);
        valueBuilders.add(obj -> {
            NavigableMap<byte[], byte[]> hBaseMap = asBytesTreeMap();
            NavigableMap<byte[], byte[]> objectMap = getter.apply(obj);
            if (objectMap == null) {
                return hBaseMap;
            }

            for (Map.Entry<byte[], byte[]> entry : objectMap.entrySet()) {
                byte[] value = entry.getValue();
                if (value == null) {
                    continue;
                }
                byte[] qualifier = ArrayUtils.addAll(prefix, entry.getKey());
                hBaseMap.put(qualifier, value);
            }

            return hBaseMap;
        });
        return this;
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix    qualifier prefix bytes
     * @param getter    lambda to extract a map-conversible value from the object
     * @param converter converts the value into a proper bytes map
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValues(byte[] prefix,
                                                        Function<T, U> getter,
                                                        Function<U, NavigableMap<byte[], byte[]>> converter) {
        return withValues(prefix, chain(getter, converter));
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix    qualifier prefix bytes
     * @param getter    lambda to extract a map-conversible value from the object
     * @param converter converts the value into a proper bytes map
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValues(byte[] prefix,
                                                        Function<T, U> getter,
                                                        BytesMapConverter<U> converter) {
        return withValues(prefix, getter, converter::toBytesMap);
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix qualifier prefix UTF-8 string
     * @param getter lambda to extract a map of (qualifier suffix -> cell value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValues(String prefix, Function<T, NavigableMap<byte[], byte[]>> getter) {
        return withValues(prefix.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix    qualifier prefix UTF-8 string
     * @param getter    lambda to extract a map-conversible value from the object
     * @param converter converts the value into a proper bytes map
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValues(String prefix,
                                                        Function<T, U> getter,
                                                        Function<U, NavigableMap<byte[], byte[]>> converter) {
        return withValues(prefix.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix    qualifier prefix UTF-8 string
     * @param getter    lambda to extract a map-conversible value from the object
     * @param converter converts the value into a proper bytes map
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValues(String prefix,
                                                        Function<T, U> getter,
                                                        BytesMapConverter<U> converter) {
        return withValues(prefix.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field  qualifier bytes
     * @param getter lambda to extract a byte[] cell value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValue(byte[] field, Function<T, byte[]> getter) {
        return withValues(field, obj -> {
            byte[] value = getter.apply(obj);
            NavigableMap<byte[], byte[]> cellsMap = asBytesTreeMap();
            if (value != null) {
                cellsMap.put(EMPTY, value);
            }
            return cellsMap;
        });
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field     qualifier bytes
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a proper {@code byte[]}
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValue(byte[] field, Function<T, U> getter, Function<U, byte[]> converter) {
        return withValue(field, chain(getter, converter));
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field     qualifier bytes
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a proper {@code byte[]}
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValue(byte[] field, Function<T, U> getter, BytesConverter<U> converter) {
        return withValue(field, getter, converter::toBytes);
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field  qualifier UTF-8 string
     * @param getter lambda to extract a byte[] cell value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValue(String field, Function<T, byte[]> getter) {
        return withValue(field.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field     qualifier UTF-8 string
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a proper {@code byte[]}
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValue(String field, Function<T, U> getter, Function<U, byte[]> converter) {
        return withValue(field.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field     qualifier UTF-8 string
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a proper {@code byte[]}
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withValue(String field, Function<T, U> getter, BytesConverter<U> converter) {
        return withValue(field.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix qualifier prefix bytes
     * @param getter lambda to extract a map of (qualifier suffix -> increment value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDeltas(byte[] prefix, Function<T, NavigableMap<byte[], Long>> getter) {
        timestampBuilders.put(prefix, currentTimestampBuilder);
        deltaBuilders.add(obj -> {
            NavigableMap<byte[], Long> hBaseMap = asBytesTreeMap();
            NavigableMap<byte[], Long> objectMap = getter.apply(obj);
            if (objectMap == null) {
                return hBaseMap;
            }

            for (Map.Entry<byte[], Long> entry : objectMap.entrySet()) {
                Long value = entry.getValue();
                if (value == null) {
                    continue;
                }
                byte[] qualifier = ArrayUtils.addAll(prefix, entry.getKey());
                hBaseMap.put(qualifier, value);
            }

            return hBaseMap;
        });
        return this;
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix    qualifier prefix bytes
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a map (qualifier suffix -> increment value)
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDeltas(byte[] prefix,
                                                        Function<T, U> getter,
                                                        Function<U, NavigableMap<byte[], Long>> converter) {
        return withDeltas(prefix, chain(getter, converter));
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix    qualifier prefix bytes
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a map (qualifier suffix -> increment value)
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDeltas(byte[] prefix,
                                                        Function<T, U> getter,
                                                        LongMapConverter<U> converter) {
        return withDeltas(prefix, getter, converter::toLongMap);
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix qualifier prefix UTF-8 string
     * @param getter lambda to extract a map of (qualifier suffix -> increment value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDeltas(String prefix, Function<T, NavigableMap<byte[], Long>> getter) {
        return withDeltas(prefix.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix    qualifier prefix UTF-8 string
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a map (qualifier suffix -> increment value)
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDeltas(String prefix,
                                                        Function<T, U> getter,
                                                        Function<U, NavigableMap<byte[], Long>> converter) {
        return withDeltas(prefix.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix    qualifier prefix UTF-8 string
     * @param getter    lambda to extract a value from the object
     * @param converter converts the value into a map (qualifier suffix -> increment value)
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDeltas(String prefix,
                                                        Function<T, U> getter,
                                                        LongMapConverter<U> converter) {
        return withDeltas(prefix.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field  qualifier bytes
     * @param getter lambda to extract a Long increment value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDelta(byte[] field, Function<T, Long> getter) {
        return withDeltas(field, obj -> {
            Long value = getter.apply(obj);
            NavigableMap<byte[], Long> cellsMap = asBytesTreeMap();
            if (value != null) {
                cellsMap.put(EMPTY, value);
            }
            return cellsMap;
        });
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field     qualifier bytes
     * @param getter    lambda to extract a increment value from the object
     * @param converter the value into a proper {@code Long}
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDelta(byte[] field, Function<T, U> getter, Function<U, Long> converter) {
        return withDelta(field, chain(getter, converter));
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field     qualifier bytes
     * @param getter    lambda to extract a increment value from the object
     * @param converter the value into a proper {@code Long}
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDelta(byte[] field, Function<T, U> getter, LongConverter<U> converter) {
        return withDelta(field, getter, converter::toLong);
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field  qualifier UTF-8 string
     * @param getter lambda to extract a Long increment value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDelta(String field, Function<T, Long> getter) {
        return withDelta(field.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field     qualifier UTF-8 string
     * @param getter    lambda to extract a increment value from the object
     * @param converter the value into a proper {@code Long}
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDelta(String field, Function<T, U> getter, Function<U, Long> converter) {
        return withDelta(field.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field     qualifier UTF-8 string
     * @param getter    lambda to extract a increment value from the object
     * @param converter the value into a proper {@code Long}
     * @return this builder
     */
    public <U> HBaseMutationSchemaBuilder<T> withDelta(String field, Function<T, U> getter, LongConverter<U> converter) {
        return withDelta(field.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    /**
     * Builds a new instance of the mutation schema
     *
     * @return new mutation schema instance
     */
    public HBaseMutationSchema<T> build() {
        verifyNonNull("No row key builder", rowKeyBuilder);
        verifyNonNull("No timestamp builder", timestampBuilder);
        verifyNonEmpty("No value or delta builder", valueBuilders, deltaBuilders);

        return new HBaseMutationSchema<T>() {
            @Override
            public byte @Nullable [] buildRowKey(T object) {
                return rowKeyBuilder.apply(object);
            }

            @Nullable
            @Override
            public Long buildTimestamp(T object) {
                return timestampBuilder.apply(object);
            }

            @Nullable
            @Override
            public Long buildTimestamp(T object, byte[] qualifier) {
                return buildCellTimestamp(object, qualifier);
            }

            @Override
            public NavigableMap<byte[], byte[]> buildPutValues(T object) {
                NavigableMap<byte[], byte[]> values = asBytesTreeMap();
                for (Function<T, NavigableMap<byte[], byte[]>> getter : valueBuilders) {
                    values.putAll(getter.apply(object));
                }
                return values;
            }

            @Override
            public NavigableMap<byte[], Long> buildIncrementValues(T object) {
                NavigableMap<byte[], Long> values = asBytesTreeMap();
                for (Function<T, NavigableMap<byte[], Long>> getter : deltaBuilders) {
                    values.putAll(getter.apply(object));
                }
                return values;
            }
        };
    }

    private Long buildCellTimestamp(T object, byte @Nullable [] qualifier) {
        if (qualifier == null) {
            return timestampBuilder.apply(object);
        }
        Map.Entry<byte[], Function<T, Long>> entry = timestampBuilders.floorEntry(qualifier);
        if (entry == null) {
            return timestampBuilder.apply(object);
        }
        byte[] prefix = entry.getKey();
        if (!prefixMatches(qualifier, prefix)) {
            return timestampBuilder.apply(object);
        }
        return entry.getValue().apply(object);
    }

    private static boolean prefixMatches(byte[] arr, byte[] prefix) {
        if (arr.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; ++i) {
            if (arr[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

}
