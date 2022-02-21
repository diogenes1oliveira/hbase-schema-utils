package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.converters.HBaseBytesConverter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseLongConverter;
import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import org.apache.hadoop.hbase.util.Pair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseBytesMappingUtils.*;

public class HBasePojoWriteSchemaBuilder<T> {
    private HBaseBytesMapper<T> rowKeyMapper = null;
    private HBaseLongMapper<T> timestampMapper = instantLongMapper(obj -> Instant.now());
    private final List<Pair<String, HBaseBytesMapper<T>>> valueFields = new ArrayList<>();
    private final List<Pair<String, HBaseLongMapper<T>>> deltaFields = new ArrayList<>();

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
        valueFields.add(Pair.newPair(name, mapper));
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
        valueFields.add(Pair.newPair(name, mapper));
        return this;
    }

    public <F> HBasePojoWriteSchemaBuilder<T> withDeltaField(
            String name, Function<T, F> fieldGetter, HBaseLongConverter<F> fieldLongConverter
    ) {
        return withDeltaField(name, longMapper(fieldGetter, fieldLongConverter.toLong()));
    }

    public AbstractHBasePojoWriteSchema<T> build() {
        if (rowKeyMapper == null) {
            throw new IllegalStateException("No row key mapper was set");
        }
        return new AbstractHBasePojoWriteSchema<T>() {
            @Override
            public List<Pair<String, HBaseBytesMapper<T>>> getPojoValueFields() {
                return valueFields;
            }

            @Override
            public List<Pair<String, HBaseLongMapper<T>>> getPojoDeltaFields() {
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
        };
    }
}
