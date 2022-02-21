package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.converters.HBaseBytesConverter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseLongConverter;
import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import hbase.schema.api.models.HBaseTypedBytesField;
import hbase.schema.api.models.HBaseTypedLongField;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseBytesMappingUtils.bytesMapper;
import static hbase.schema.api.utils.HBaseBytesMappingUtils.instantLongMapper;
import static hbase.schema.api.utils.HBaseBytesMappingUtils.longMapper;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static java.util.stream.Collectors.toMap;

public class HBasePojoWriteSchemaBuilder<T> {
    private final Supplier<T> objectSupplier;
    private HBaseBytesMapper<T> rowKeyMapper = null;
    private HBaseLongMapper<T> timestampMapper = instantLongMapper(obj -> Instant.now());
    private final TreeMap<String, HBaseTypedBytesField.Builder<T>> valueBuilders = new TreeMap<>();
    private final TreeMap<String, HBaseTypedLongField.Builder<T>> deltaBuilders = new TreeMap<>();

    public HBasePojoWriteSchemaBuilder(Supplier<T> objectSupplier) {
        this.objectSupplier = objectSupplier;
    }

    public HBasePojoWriteSchemaBuilder<T> withRowKey(HBaseBytesMapper<T> rowKeyMapper) {
        this.rowKeyMapper = rowKeyMapper;
        return this;
    }

    public <F> HBasePojoWriteSchemaBuilder<T> withRowKey(Function<T, F> fieldGetter, HBaseBytesConverter<F> fieldBytesConverter) {
        HBaseBytesMapper<T> mapper = bytesMapper(fieldGetter, fieldBytesConverter.toBytes());
        return withRowKey(mapper);
    }

    public HBasePojoWriteSchemaBuilder<T> withTimestamp(HBaseLongMapper<T> timestampMapper) {
        this.timestampMapper = timestampMapper;
        return this;
    }

    public <F> HBasePojoWriteSchemaBuilder<T> withTimestamp(Function<T, F> fieldGetter, HBaseLongConverter<F> fieldLongConverter) {
        HBaseLongMapper<T> mapper = longMapper(fieldGetter, fieldLongConverter.toLong());
        return withTimestamp(mapper);
    }

    public HBasePojoWriteSchemaBuilder<T> withValueField(
            String name, HBaseBytesMapper<T> mapper
    ) {
        HBaseTypedBytesField.Builder<T> builder = valueBuilders.computeIfAbsent(name, HBaseTypedBytesField.Builder::new);
        builder.withMapper(mapper);
        return this;
    }

    public <F> HBasePojoWriteSchemaBuilder<T> withValueField(
            String name, Function<T, F> fieldGetter, HBaseBytesConverter<F> fieldBytesConverter
    ) {
        return withValueField(name, bytesMapper(fieldGetter, fieldBytesConverter.toBytes()));
    }

    public HBasePojoWriteSchemaBuilder<T> withDeltaField(
            String name, HBaseLongMapper<T> mapper
    ) {
        HBaseTypedLongField.Builder<T> builder = deltaBuilders.computeIfAbsent(name, HBaseTypedLongField.Builder::new);
        builder.withMapper(mapper);
        return this;
    }

    public <F> HBasePojoWriteSchemaBuilder<T> withDeltaField(
            String name, Function<T, F> fieldGetter, HBaseLongConverter<F> fieldLongConverter
    ) {
        return withDeltaField(name, longMapper(fieldGetter, fieldLongConverter.toLong()));
    }
//
//    public <F> HBasePojoWriteSchemaBuilder<T> withDeltaField(
//            String name, Function<T, Long> longGetter
//    ) {
//        HBaseLongMapper<T> mapper = longMapper(longGetter, Function.identity());
//        return withDeltaField(name, mapper);
//    }

    public AbstractHBasePojoWriteSchema<T> build() {
        if (rowKeyMapper == null) {
            throw new IllegalStateException("No row key mapper was set");
        }
        TreeMap<byte[], HBaseTypedBytesField<T>> valueFields = mapValuesStringMap(valueBuilders, HBaseTypedBytesField.Builder::build);
        TreeMap<byte[], HBaseTypedLongField<T>> deltaFields = mapValuesStringMap(deltaBuilders, HBaseTypedLongField.Builder::build);

        return new AbstractHBasePojoWriteSchema<T>() {
            @Override
            public NavigableMap<byte[], HBaseTypedBytesField<T>> getPojoValueFields() {
                return valueFields;
            }

            @Override
            public NavigableMap<byte[], HBaseTypedLongField<T>> getPojoDeltaFields() {
                return deltaFields;
            }

            @Override
            public HBaseBytesMapper<T> getRowKeyGenerator() {
                return rowKeyMapper;
            }

            @Override
            public HBaseLongMapper<T> getTimestampGenerator() {
                return timestampMapper;
            }

            @Override
            public T newInstance() {
                return objectSupplier.get();
            }
        };
    }

    private static <T, U> TreeMap<byte[], U> mapValuesStringMap(Map<String, T> input, Function<T, U> mapper) {
        TreeMap<byte[], U> output = asBytesTreeMap();
        for (Map.Entry<String, T> entry : input.entrySet()) {
            byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
            T inputValue = entry.getValue();
            U mappedValue = mapper.apply(inputValue);
            output.put(key, mappedValue);
        }
        return output;
    }

    public static <K, V, U> LinkedHashMap<K, V> mapBytesMap(Map<byte[], U> input,
                                                            Function<byte[], K> keyMapper,
                                                            Function<U, V> valueMapper) {
        return input.entrySet()
                    .stream()
                    .collect(toMap(e -> keyMapper.apply(e.getKey()), e -> valueMapper.apply(e.getValue()), (a, b) -> a, LinkedHashMap::new));
    }
}
