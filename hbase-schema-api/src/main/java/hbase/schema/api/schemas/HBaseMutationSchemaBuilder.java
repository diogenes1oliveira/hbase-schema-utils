package hbase.schema.api.schemas;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.converters.HBaseBytesGetter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapGetter;
import hbase.schema.api.interfaces.converters.HBaseLongGetter;
import hbase.schema.api.interfaces.converters.HBaseLongMapGetter;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonEmpty;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonNull;

/**
 * Builder for {@link HBaseMutationSchema} objects, providing a fluent API to add fields and field prefixes to be inserted
 *
 * @param <T> result object instance
 */
public class HBaseMutationSchemaBuilder<T> {
    private static final byte[] EMPTY = new byte[0];
    private HBaseBytesGetter<T> rowKeyBuilder = null;
    private HBaseLongGetter<T> timestampBuilder = null;

    private final List<HBaseBytesMapGetter<T>> valueBuilders = new ArrayList<>();
    private final List<HBaseLongMapGetter<T>> deltaBuilders = new ArrayList<>();
    private final NavigableMap<byte[], HBaseLongGetter<T>> timestampBuilders = asBytesTreeMap();

    /**
     * Sets up the row key generation
     *
     * @param getter lambda to get the row key bytes from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withRowKey(HBaseBytesGetter<T> getter) {
        this.rowKeyBuilder = getter;
        return this;
    }

    /**
     * Sets up the timestamp generator
     *
     * @param getter lambda to get the cell timestamp as milliseconds value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withTimestamp(HBaseLongGetter<T> getter) {
        this.timestampBuilder = getter;
        return this;
    }

    /**
     * Sets up a set of cells to be inserted as HBase values
     *
     * @param prefix qualifier prefix bytes
     * @param getter lambda to extract a map of (qualifier suffix -> cell value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValues(byte[] prefix, HBaseBytesMapGetter<T> getter) {
        timestampBuilders.put(prefix, timestampBuilder);
        valueBuilders.add(obj -> {
            NavigableMap<byte[], byte[]> valuesMap = asBytesTreeMap();

            for (Map.Entry<byte[], byte[]> entry : getter.getBytesMap(obj).entrySet()) {
                byte[] value = entry.getValue();
                if (value == null) {
                    continue;
                }
                byte[] qualifier = ArrayUtils.addAll(prefix, entry.getKey());
                valuesMap.put(qualifier, value);
            }

            return valuesMap;
        });
        return this;
    }

    /**
     * Sets up a set of cells to be inserted as HBase Puts
     *
     * @param prefix qualifier prefix UTF-8 string
     * @param getter lambda to extract a map of (qualifier suffix -> cell value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValues(String prefix, HBaseBytesMapGetter<T> getter) {
        return withValues(prefix.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a single cell to be inserted in a HBase Put
     *
     * @param field  qualifier bytes
     * @param getter lambda to extract a byte[] cell value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValue(byte[] field, HBaseBytesGetter<T> getter) {
        return withValues(field, obj -> {
            byte[] value = getter.getBytes(obj);
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
     * @param field  qualifier UTF-8 string
     * @param getter lambda to extract a byte[] cell value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withValue(String field, HBaseBytesGetter<T> getter) {
        return withValue(field.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix qualifier prefix bytes
     * @param getter lambda to extract a map of (qualifier suffix -> increment value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDeltas(byte[] prefix, HBaseLongMapGetter<T> getter) {
        timestampBuilders.put(prefix, timestampBuilder);
        deltaBuilders.add(obj -> {
            NavigableMap<byte[], Long> valuesMap = asBytesTreeMap();

            for (Map.Entry<byte[], Long> entry : getter.getLongMap(obj).entrySet()) {
                Long value = entry.getValue();
                if (value == null) {
                    continue;
                }
                byte[] qualifier = ArrayUtils.addAll(prefix, entry.getKey());
                valuesMap.put(qualifier, value);
            }

            return valuesMap;
        });
        return this;
    }

    /**
     * Sets up a set of cells to be updated as HBase Increments
     *
     * @param prefix qualifier prefix UTF-8 string
     * @param getter lambda to extract a map of (qualifier suffix -> increment value) from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDeltas(String prefix, HBaseBytesMapGetter<T> getter) {
        return withValues(prefix.getBytes(StandardCharsets.UTF_8), getter);
    }

    /**
     * Sets up a single cell to be updated in a HBase Increment
     *
     * @param field  qualifier bytes
     * @param getter lambda to extract a Long increment value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDelta(byte[] field, HBaseLongGetter<T> getter) {
        return withDeltas(field, obj -> {
            Long value = getter.getLong(obj);
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
     * @param field  qualifier UTF-8 string
     * @param getter lambda to extract a Long increment value from the object
     * @return this builder
     */
    public HBaseMutationSchemaBuilder<T> withDelta(String field, HBaseLongGetter<T> getter) {
        return withDelta(field.getBytes(StandardCharsets.UTF_8), getter);
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
            @Nullable
            @Override
            public byte[] buildRowKey(T object) {
                return rowKeyBuilder.getBytes(object);
            }

            @Nullable
            @Override
            public Long buildTimestamp(T object) {
                return timestampBuilder.getLong(object);
            }

            @Nullable
            @Override
            public Long buildTimestamp(T object, byte[] qualifier) {
                return buildCellTimestamp(object, qualifier);
            }

            @Override
            public NavigableMap<byte[], byte[]> buildCellValues(T object) {
                NavigableMap<byte[], byte[]> values = asBytesTreeMap();
                for (HBaseBytesMapGetter<T> getter : valueBuilders) {
                    values.putAll(getter.getBytesMap(object));
                }
                return values;
            }

            @Override
            public NavigableMap<byte[], Long> buildCellIncrements(T object) {
                NavigableMap<byte[], Long> values = asBytesTreeMap();
                for (HBaseLongMapGetter<T> getter : deltaBuilders) {
                    values.putAll(getter.getLongMap(object));
                }
                return values;
            }
        };
    }

    private Long buildCellTimestamp(T object, @Nullable byte[] qualifier) {
        if (qualifier == null) {
            return timestampBuilder.getLong(object);
        }
        Map.Entry<byte[], HBaseLongGetter<T>> entry = timestampBuilders.ceilingEntry(qualifier);
        if (entry == null) {
            return timestampBuilder.getLong(object);
        }
        byte[] prefix = entry.getKey();
        if (!prefixMatches(qualifier, prefix)) {
            return timestampBuilder.getLong(object);
        }
        return entry.getValue().getLong(object);
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
