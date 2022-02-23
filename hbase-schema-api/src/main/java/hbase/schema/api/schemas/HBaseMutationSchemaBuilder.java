package hbase.schema.api.schemas;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.schema.api.interfaces.HBaseMutationSchema;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonEmpty;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonNull;

public class HBaseMutationSchemaBuilder<T> {
    private Function<T, byte[]> rowKeyBuilder = null;
    private Function<T, Long> timestampBuilder = null;

    private final List<Function<T, NavigableMap<byte[], byte[]>>> valueBuilders = new ArrayList<>();
    private final List<Function<T, NavigableMap<byte[], Long>>> deltaBuilders = new ArrayList<>();
    private final NavigableMap<byte[], Function<T, Long>> timestampBuilders = asBytesTreeMap();

    public HBaseMutationSchemaBuilder<T> withRowKey(Function<T, byte[]> getter) {
        this.rowKeyBuilder = getter;
        return this;
    }

    public HBaseMutationSchemaBuilder<T> withTimestamp(Function<T, Long> getter) {
        this.timestampBuilder = getter;
        return this;
    }

    public HBaseMutationSchemaBuilder<T> withValues(byte[] prefix,
                                                    Function<T, NavigableMap<byte[], byte[]>> getter) {
        timestampBuilders.put(prefix, timestampBuilder);
        valueBuilders.add(obj -> {
            NavigableMap<byte[], byte[]> valuesMap = asBytesTreeMap();

            for (Map.Entry<byte[], byte[]> entry : getter.apply(obj).entrySet()) {
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

    public HBaseMutationSchemaBuilder<T> withValues(String prefix,
                                                    Function<T, NavigableMap<byte[], byte[]>> getter) {
        return withValues(prefix.getBytes(StandardCharsets.UTF_8), getter);
    }

    public HBaseMutationSchemaBuilder<T> withValue(byte[] field, Function<T, byte[]> getter) {
        timestampBuilders.put(field, timestampBuilder);
        valueBuilders.add(obj -> {
            NavigableMap<byte[], byte[]> map = asBytesTreeMap();
            byte[] value = getter.apply(obj);
            if (value != null) {
                map.put(field, value);
            }
            return map;
        });
        return this;
    }

    public HBaseMutationSchemaBuilder<T> withValue(String field, Function<T, byte[]> getter) {
        return withValue(field.getBytes(StandardCharsets.UTF_8), getter);
    }

    public HBaseMutationSchemaBuilder<T> withDelta(byte[] field, Function<T, Long> getter) {
        timestampBuilders.put(field, timestampBuilder);
        deltaBuilders.add(obj -> {
            NavigableMap<byte[], Long> map = asBytesTreeMap();
            Long value = getter.apply(obj);
            if (value != null) {
                map.put(field, value);
            }
            return map;
        });
        return this;
    }

    public HBaseMutationSchemaBuilder<T> withDelta(String field, Function<T, Long> getter) {
        return withDelta(field.getBytes(StandardCharsets.UTF_8), getter);
    }

    public HBaseMutationSchema<T> build() {
        verifyNonNull("No row key builder", rowKeyBuilder);
        verifyNonNull("No timestamp builder", timestampBuilder);
        verifyNonEmpty("No value or delta builder", valueBuilders, deltaBuilders);

        return new HBaseFunctionalMutationSchema<T>(
                rowKeyBuilder,
                this::buildTimestamp,
                valueBuilders,
                deltaBuilders
        );
    }

    private Long buildTimestamp(T object, @Nullable byte[] qualifier) {
        if (qualifier == null) {
            return timestampBuilder.apply(object);
        }
        Map.Entry<byte[], Function<T, Long>> entry = timestampBuilders.ceilingEntry(qualifier);
        if (entry == null) {
            return timestampBuilder.apply(object);
        }
        return entry.getValue().apply(object);
    }
}
