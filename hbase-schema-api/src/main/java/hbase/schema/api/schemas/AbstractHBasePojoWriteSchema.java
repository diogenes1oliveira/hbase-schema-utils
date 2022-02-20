package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.*;
import org.apache.hadoop.hbase.util.Triple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static hbase.schema.api.utils.HBaseSchemaUtils.frozenSortedByteMap;

public abstract class AbstractHBasePojoWriteSchema<T> implements HBaseWriteSchema<T> {
    public abstract List<Triple<String, HBaseBytesParser<T>, HBaseBytesMapper<T>>> getPojoValueFields();

    public abstract List<Triple<String, HBaseLongParser<T>, HBaseLongMapper<T>>> getPojoDeltaFields();

    @Override
    public abstract HBaseBytesMapper<T> getRowKeyGenerator();

    @Override
    public abstract HBaseLongMapper<T> getTimestampGenerator();

    @Override
    public List<HBaseCellsMapper<T>> getPutGenerators() {
        List<HBaseCellsMapper<T>> mappers = new ArrayList<>();

        for (Triple<String, HBaseBytesParser<T>, HBaseBytesMapper<T>> pojoValueField : getPojoValueFields()) {
            byte[] qualifier = pojoValueField.getFirst().getBytes(StandardCharsets.UTF_8);
            HBaseBytesMapper<T> bytesMapper = pojoValueField.getThird();
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

        for (Triple<String, HBaseLongParser<T>, HBaseLongMapper<T>> pojoDeltaField : getPojoDeltaFields()) {
            byte[] qualifier = pojoDeltaField.getFirst().getBytes(StandardCharsets.UTF_8);
            HBaseLongMapper<T> longMapper = pojoDeltaField.getThird();
            HBaseLongsMapper<T> cellsMapper = obj -> {
                Long value = longMapper.getLong(obj);
                return frozenSortedByteMap(qualifier, value);
            };
            mappers.add(cellsMapper);
        }

        return mappers;
    }


    public static <T> Triple<String, HBaseBytesParser<T>, HBaseBytesMapper<T>> pojoValueField(
            String name,
            HBaseBytesParser<T> parser,
            HBaseBytesMapper<T> mapper
    ) {
        return Triple.create(name, parser, mapper);
    }

    public static <T> Triple<String, HBaseLongParser<T>, HBaseLongMapper<T>> pojoDeltaField(
            String name,
            HBaseLongParser<T> parser,
            HBaseLongMapper<T> mapper
    ) {
        return Triple.create(name, parser, mapper);
    }
}
