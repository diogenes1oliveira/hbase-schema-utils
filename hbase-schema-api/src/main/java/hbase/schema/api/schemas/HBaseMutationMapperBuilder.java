package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.interfaces.conversion.BytesMapConverter;
import hbase.schema.api.interfaces.conversion.LongConverter;
import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;

public class HBaseMutationMapperBuilder<T> {
    private Function<T, Long> rowTimestamper = null;
    private Function<T, Long> timestamper = t -> null;
    private Function<T, byte[]> rowKeyMapper = null;
    private final List<Function<T, List<HBaseValueCell>>> cellBuilders = new ArrayList<>();
    private final List<Function<T, List<HBaseDeltaCell>>> deltaBuilders = new ArrayList<>();
    private final Set<byte[]> qualifiers = asBytesTreeSet();
    private final Set<byte[]> prefixes = asBytesTreeSet();

    public HBaseMutationMapperBuilder<T> rowKey(Function<T, byte[]> rowKeyMapper) {
        this.rowTimestamper = timestamper;
        this.rowKeyMapper = rowKeyMapper;
        return this;
    }

    public <F> HBaseMutationMapperBuilder<T> rowKey(Function<T, F> getter, Function<F, byte[]> converter) {
        return rowKey(chain(getter, converter));
    }

    public <F> HBaseMutationMapperBuilder<T> rowKey(Function<T, F> rowKeyMapper, BytesConverter<F> converter) {
        return rowKey(rowKeyMapper, converter::toBytes);
    }

    public HBaseMutationMapperBuilder<T> timestampLong(Function<T, Long> timestampMapper) {
        this.timestamper = timestampMapper;
        return this;
    }

    public <F> HBaseMutationMapperBuilder<T> timestamp(Function<T, F> getter, Function<F, Long> converter) {
        return timestampLong(chain(getter, converter));
    }

    public <F> HBaseMutationMapperBuilder<T> timestamp(Function<T, F> rowKeyMapper, LongConverter<F> converter) {
        return timestamp(rowKeyMapper, converter::toLong);
    }

    public HBaseMutationMapperBuilder<T> columnBytes(byte[] qualifier, Function<T, byte[]> valueMapper) {
        Function<T, Long> columnTimestamper = timestamper;
        this.qualifiers.add(qualifier);
        this.cellBuilders.add(obj -> {
            byte[] value = valueMapper.apply(obj);
            if (value != null) {
                Long timestamp = columnTimestamper.apply(obj);
                return singletonList(new HBaseValueCell(qualifier, value, timestamp));
            } else {
                return emptyList();
            }
        });
        return this;
    }

    public <F> HBaseMutationMapperBuilder<T> column(byte[] qualifier, Function<T, F> getter, Function<F, byte[]> converter) {
        return columnBytes(qualifier, chain(getter, converter));
    }

    public <F> HBaseMutationMapperBuilder<T> column(byte[] qualifier, Function<T, F> getter, BytesConverter<F> converter) {
        return column(qualifier, getter, converter::toBytes);
    }

    public HBaseMutationMapperBuilder<T> columnBytes(String qualifier, Function<T, byte[]> valueMapper) {
        return columnBytes(qualifier.getBytes(StandardCharsets.UTF_8), valueMapper);
    }

    public <F> HBaseMutationMapperBuilder<T> column(String qualifier, Function<T, F> getter, Function<F, byte[]> converter) {
        return column(qualifier.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    public <F> HBaseMutationMapperBuilder<T> column(String qualifier, Function<T, F> getter, BytesConverter<F> converter) {
        return column(qualifier.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    public HBaseMutationMapperBuilder<T> deltaLong(byte[] qualifier, Function<T, Long> valueMapper) {
        this.qualifiers.add(qualifier);
        this.deltaBuilders.add(obj -> {
            Long value = valueMapper.apply(obj);
            if (value != null && value != 0L) {
                return singletonList(new HBaseDeltaCell(qualifier, value));
            } else {
                return emptyList();
            }
        });
        return this;
    }

    public <F> HBaseMutationMapperBuilder<T> delta(byte[] qualifier, Function<T, F> getter, Function<F, Long> converter) {
        return deltaLong(qualifier, chain(getter, converter));
    }

    public <F> HBaseMutationMapperBuilder<T> delta(byte[] qualifier, Function<T, F> getter, LongConverter<F> converter) {
        return delta(qualifier, getter, converter::toLong);
    }

    public HBaseMutationMapperBuilder<T> deltaLong(String qualifier, Function<T, Long> valueMapper) {
        return deltaLong(qualifier.getBytes(StandardCharsets.UTF_8), valueMapper);
    }

    public <F> HBaseMutationMapperBuilder<T> delta(String qualifier, Function<T, F> getter, Function<F, Long> converter) {
        return delta(qualifier.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    public <F> HBaseMutationMapperBuilder<T> delta(String qualifier, Function<T, F> getter, LongConverter<F> converter) {
        return delta(qualifier.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    public HBaseMutationMapperBuilder<T> prefixBytes(byte[] prefix, Function<T, NavigableMap<byte[], byte[]>> cellsMapper) {
        this.prefixes.add(prefix);
        Function<T, Long> columnTimestamper = timestamper;
        this.cellBuilders.add(obj -> {
            NavigableMap<byte[], byte[]> prefixMap = cellsMapper.apply(obj);
            if (prefixMap == null) {
                return emptyList();
            }
            Long timestamp = columnTimestamper.apply(obj);
            return HBaseValueCell.fromPrefixMap(prefix, timestamp, prefixMap);
        });
        return this;
    }

    public <F> HBaseMutationMapperBuilder<T> prefix(byte[] qualifier,
                                                    Function<T, F> getter,
                                                    Function<F, NavigableMap<byte[], byte[]>> converter) {
        return prefixBytes(qualifier, chain(getter, converter));
    }

    public <F> HBaseMutationMapperBuilder<T> prefix(byte[] qualifier,
                                                    Function<T, F> getter,
                                                    BytesMapConverter<F> converter) {
        return prefix(qualifier, getter, converter::toBytesMap);
    }

    public HBaseMutationMapperBuilder<T> prefixBytes(String qualifier, Function<T, NavigableMap<byte[], byte[]>> valueMapper) {
        return prefixBytes(qualifier.getBytes(StandardCharsets.UTF_8), valueMapper);
    }

    public <F> HBaseMutationMapperBuilder<T> prefix(String qualifier,
                                                    Function<T, F> getter,
                                                    Function<F, NavigableMap<byte[], byte[]>> converter) {
        return prefix(qualifier.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    public <F> HBaseMutationMapperBuilder<T> prefix(String qualifier, Function<T, F> getter, BytesMapConverter<F> converter) {
        return prefix(qualifier.getBytes(StandardCharsets.UTF_8), getter, converter);
    }

    public HBaseMutationMapper<T> build() {
        Set<byte[]> qualifiers = unmodifiableSet(this.qualifiers);
        Set<byte[]> prefixes = unmodifiableSet(this.prefixes);

        return new HBaseMutationMapper<T>() {
            @Override
            public @NotNull Set<byte[]> prefixes() {
                return prefixes;
            }

            @Override
            public @NotNull Set<byte[]> qualifiers() {
                return qualifiers;
            }

            @Override
            public byte @Nullable [] toRowKey(T obj) {
                return rowKeyMapper.apply(obj);
            }

            @Override
            public @Nullable Long toTimestamp(T obj) {
                return rowTimestamper.apply(obj);
            }

            @Override
            public List<HBaseDeltaCell> toDeltas(T obj) {
                List<HBaseDeltaCell> cells = new ArrayList<>();
                for (Function<T, List<HBaseDeltaCell>> builder : deltaBuilders) {
                    cells.addAll(builder.apply(obj));
                }
                return cells;
            }

            @Override
            public List<HBaseValueCell> toValues(T obj) {
                List<HBaseValueCell> cells = new ArrayList<>();
                for (Function<T, List<HBaseValueCell>> builder : cellBuilders) {
                    cells.addAll(builder.apply(obj));
                }
                return cells;
            }
        };
    }
}
