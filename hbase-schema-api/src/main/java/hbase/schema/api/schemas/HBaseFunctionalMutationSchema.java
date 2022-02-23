package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationSchema;

import java.util.List;
import java.util.NavigableMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

public class HBaseFunctionalMutationSchema<T> implements HBaseMutationSchema<T> {
    private final Function<T, byte[]> rowKeyBuilder;
    private final BiFunction<T, byte[], Long> timestampBuilder;
    private final List<Function<T, NavigableMap<byte[], byte[]>>> valueBuilders;
    private final List<Function<T, NavigableMap<byte[], Long>>> deltaBuilders;

    public HBaseFunctionalMutationSchema(Function<T, byte[]> rowKeyBuilder,
                                         BiFunction<T, byte[], Long> timestampBuilder,
                                         List<Function<T, NavigableMap<byte[], byte[]>>> valueBuilders,
                                         List<Function<T, NavigableMap<byte[], Long>>> deltaBuilders) {
        this.rowKeyBuilder = rowKeyBuilder;
        this.timestampBuilder = timestampBuilder;
        this.valueBuilders = valueBuilders;
        this.deltaBuilders = deltaBuilders;
    }

    @Override
    public byte[] buildRowKey(T object) {
        return rowKeyBuilder.apply(object);
    }

    @Override
    public long buildTimestamp(T object) {
        return timestampBuilder.apply(object, null);
    }

    @Override
    public long buildTimestamp(T object, byte[] qualifier) {
        Long cellTimestamp = timestampBuilder.apply(object, qualifier);
        if (cellTimestamp == null) {
            return buildTimestamp(object);
        } else {
            return cellTimestamp;
        }
    }

    @Override
    public NavigableMap<byte[], byte[]> buildCellValues(T object) {
        NavigableMap<byte[], byte[]> valuesMap = asBytesTreeMap();

        for (Function<T, NavigableMap<byte[], byte[]>> builder : valueBuilders) {
            NavigableMap<byte[], byte[]> builderValuesMap = builder.apply(object);
            valuesMap.putAll(builderValuesMap);
        }

        return valuesMap;
    }

    @Override
    public NavigableMap<byte[], Long> buildCellIncrements(T object) {
        NavigableMap<byte[], Long> valuesMap = asBytesTreeMap();

        for (Function<T, NavigableMap<byte[], Long>> builder : deltaBuilders) {
            NavigableMap<byte[], Long> builderValuesMap = builder.apply(object);
            valuesMap.putAll(builderValuesMap);
        }

        return valuesMap;
    }
}
