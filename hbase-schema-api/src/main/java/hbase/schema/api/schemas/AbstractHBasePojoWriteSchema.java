package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.*;
import hbase.schema.api.models.HBaseTypedBytesField;
import hbase.schema.api.models.HBaseTypedLongField;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.*;
import static java.util.Arrays.asList;

public abstract class AbstractHBasePojoWriteSchema<T> implements HBaseWriteSchema<T>, HBaseReadSchema<T> {
    public abstract NavigableMap<byte[], HBaseTypedBytesField<T>> getPojoValueFields();

    public abstract NavigableMap<byte[], HBaseTypedLongField<T>> getPojoDeltaFields();

    @Override
    public abstract HBaseBytesMapper<T> getRowKeyGenerator();

    @Override
    public abstract HBaseLongMapper<T> getTimestampGenerator();

    @Override
    public List<HBaseCellsMapper<T>> getPutGenerators() {
        List<HBaseCellsMapper<T>> mappers = new ArrayList<>();

        for (HBaseTypedBytesField<T> pojoValueField : getPojoValueFields().values()) {
            byte[] qualifier = pojoValueField.name().getBytes(StandardCharsets.UTF_8);
            HBaseBytesMapper<T> bytesMapper = pojoValueField.mapper();
            HBaseCellsMapper<T> cellsMapper = obj -> {
                byte[] value = bytesMapper.getBytes(obj);
                if (value != null) {
                    return frozenSortedByteMap(qualifier, value);
                } else {
                    return frozenSortedByteMap();
                }
            };
            mappers.add(cellsMapper);
        }

        return mappers;
    }

    @Override
    public List<HBaseLongsMapper<T>> getIncrementGenerators() {
        List<HBaseLongsMapper<T>> mappers = new ArrayList<>();

        for (HBaseTypedLongField<T> pojoDeltaField : getPojoDeltaFields().values()) {
            byte[] qualifier = pojoDeltaField.name().getBytes(StandardCharsets.UTF_8);
            HBaseLongMapper<T> longMapper = pojoDeltaField.mapper();
            HBaseLongsMapper<T> cellsMapper = obj -> {
                Long value = longMapper.getLong(obj);
                return frozenSortedByteMap(qualifier, value);
            };
            mappers.add(cellsMapper);
        }

        return mappers;
    }

    // READ FIELDS
    @Override
    public HBaseBytesParser<T> getRowKeyParser() {
        // DEFAULT
        return HBaseBytesParser.dummy();
    }

    @Override
    public HBaseBytesMapper<T> getScanRowKeyGenerator() {
        // DEFAULT
        return getRowKeyGenerator();
    }

    @Override
    public List<HBaseCellParser<T>> getCellParsers() {
        return asList(getBytesCellParser(), getDeltaCellParser());
    }

    @Override
    public abstract T newInstance();

    @Override
    public SortedSet<byte[]> getQualifiers(T query) {
        return getPojoDeltaFields().navigableKeySet();
    }

    @Override
    public SortedSet<byte[]> getQualifierPrefixes(T query) {
        // DEFAULT
        byte[] commonPrefix = findCommonPrefix(getQualifiers(query));
        if (commonPrefix != null) {
            return frozenSortedByteSet(commonPrefix);
        } else {
            return frozenSortedByteSet();
        }
    }

    private HBaseCellParser<T> getBytesCellParser() {
        NavigableMap<byte[], HBaseTypedBytesField<T>> fieldsByQualifier = getPojoValueFields();

        return (obj, qualifier, value) -> {
            HBaseTypedBytesField<T> field = fieldsByQualifier.get(qualifier);
            if (field != null) {
                field.parser().setFromBytes(obj, value);
            }
        };
    }

    private HBaseCellParser<T> getDeltaCellParser() {
        NavigableMap<byte[], HBaseTypedLongField<T>> fieldsByQualifier = getPojoDeltaFields();

        return (obj, qualifier, value) -> {
            HBaseTypedLongField<T> field = fieldsByQualifier.get(qualifier);
            if (field != null) {
                field.parser().setFromBytes(obj, value);
            }
        };
    }
}
