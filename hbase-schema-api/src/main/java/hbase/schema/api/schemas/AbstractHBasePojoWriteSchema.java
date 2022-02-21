package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseCellsMapper;
import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import hbase.schema.api.interfaces.converters.HBaseLongsMapper;
import org.apache.hadoop.hbase.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static hbase.schema.api.utils.HBaseSchemaUtils.frozenSortedByteMap;

public abstract class AbstractHBasePojoWriteSchema<T> implements HBaseWriteSchema<T> {
    public abstract List<Pair<String, HBaseBytesMapper<T>>> getPojoValueFields();

    public abstract List<Pair<String, HBaseLongMapper<T>>> getPojoDeltaFields();

    @Override
    public abstract HBaseBytesMapper<T> getRowKeyGenerator();

    @Override
    public abstract HBaseLongMapper<T> getTimestampGenerator();

    @Override
    public List<HBaseCellsMapper<T>> getPutGenerators() {
        List<HBaseCellsMapper<T>> mappers = new ArrayList<>();

        for (Pair<String, HBaseBytesMapper<T>> pojoValueField : getPojoValueFields()) {
            byte[] qualifier = pojoValueField.getFirst().getBytes(StandardCharsets.UTF_8);
            HBaseBytesMapper<T> bytesMapper = pojoValueField.getSecond();
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

        for (Pair<String, HBaseLongMapper<T>> pojoDeltaField : getPojoDeltaFields()) {
            byte[] qualifier = pojoDeltaField.getFirst().getBytes(StandardCharsets.UTF_8);
            HBaseLongMapper<T> longMapper = pojoDeltaField.getSecond();
            HBaseLongsMapper<T> cellsMapper = obj -> {
                Long value = longMapper.getLong(obj);
                return frozenSortedByteMap(qualifier, value);
            };
            mappers.add(cellsMapper);
        }

        return mappers;
    }

    public static <T> HBasePojoWriteSchemaBuilder<T> newBuilder(Class<T> type) {
        return new HBasePojoWriteSchemaBuilder<>();
    }

    public static <T> Pair<String, HBaseBytesMapper<T>> pojoValueField(
            String name,
            HBaseBytesMapper<T> mapper
    ) {
        return Pair.newPair(name, mapper);
    }

    public static <T> Pair<String, HBaseLongMapper<T>> pojoDeltaField(
            String name,
            HBaseLongMapper<T> mapper
    ) {
        return Pair.newPair(name, mapper);
    }
}
