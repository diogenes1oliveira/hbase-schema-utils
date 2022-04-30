package hbase.schema.api.schema;

import hbase.schema.api.interfaces.HBaseQueryMapper;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class HBaseQueryMapperBuilder<T> {
    private final Set<byte[]> prefixes = asBytesTreeSet();
    private final Set<byte[]> qualifiers = asBytesTreeSet();
    private Function<T, byte[]> rowKeyMapper = query -> {
        throw new UnsupportedOperationException("Get queries are not supported");
    };
    private Function<T, List<Pair<byte[], byte[]>>> rangeMapper = query -> {
        throw new UnsupportedOperationException("Scan queries are not supported");
    };

    public HBaseQueryMapperBuilder<T> prefixes(byte[] first, byte[]... rest) {
        prefixes.add(first);
        prefixes.addAll(asList(rest));
        return this;
    }

    public HBaseQueryMapperBuilder<T> prefixes(String... prefixes) {
        return prefixes(asList(prefixes));
    }

    public HBaseQueryMapperBuilder<T> prefixes(Iterable<String> prefixes) {
        for (String prefix : prefixes) {
            this.prefixes.add(prefix.getBytes(StandardCharsets.UTF_8));
        }
        return this;
    }

    public HBaseQueryMapperBuilder<T> columns(byte[] first, byte[]... rest) {
        qualifiers.add(first);
        qualifiers.addAll(asList(rest));
        return this;
    }

    public HBaseQueryMapperBuilder<T> columns(String... qualifiers) {
        return columns(asList(qualifiers));
    }

    public HBaseQueryMapperBuilder<T> columns(Iterable<String> qualifiers) {
        for (String qualifier : qualifiers) {
            this.qualifiers.add(qualifier.getBytes(StandardCharsets.UTF_8));
        }
        return this;
    }

    public HBaseQueryMapperBuilder<T> rowKey(Function<T, byte[]> rowKeyMapper) {
        this.rowKeyMapper = rowKeyMapper;
        return this;
    }

    public <F> HBaseQueryMapperBuilder<T> rowKey(Function<T, F> getter, Function<F, byte[]> converter) {
        return rowKey(chain(getter, converter));
    }

    public <F> HBaseQueryMapperBuilder<T> rowKey(Function<T, F> rowKeyMapper, BytesConverter<F> converter) {
        return rowKey(rowKeyMapper, converter::toBytes);
    }

    public HBaseQueryMapperBuilder<T> searchRanges(Function<T, List<Pair<byte[], byte[]>>> rangesMapper) {
        this.rangeMapper = query -> {
            List<Pair<byte[], byte[]>> ranges = rangesMapper.apply(query);
            return ranges == null ? emptyList() : ranges;
        };
        return this;
    }

    public <F> HBaseQueryMapperBuilder<T> searchRanges(Function<T, List<Pair<F, F>>> rangesGetter, Function<F, byte[]> converter) {
        return searchRanges(chain(rangesGetter, ranges -> {
            List<Pair<byte[], byte[]>> pairs = new ArrayList<>();
            for (Pair<F, F> range : ranges) {
                byte[] left = converter.apply(range.getLeft());
                byte[] right = converter.apply(range.getRight());
                pairs.add(Pair.of(left, right));
            }
            return pairs;
        }));
    }

    public HBaseQueryMapperBuilder<T> searchRange(Function<T, Pair<byte[], byte[]>> rangeMapper) {
        return searchRanges(query -> singletonList(rangeMapper.apply(query)));
    }

    public <F> HBaseQueryMapperBuilder<T> searchRange(Function<T, Pair<F, F>> rangeGetter, Function<F, byte[]> converter) {
        return searchRange(chain(rangeGetter, pair -> {
            byte[] left = converter.apply(pair.getLeft());
            byte[] right = converter.apply(pair.getRight());
            return Pair.of(left, right);
        }));
    }

    public <F> HBaseQueryMapperBuilder<T> searchRange(Function<T, Pair<F, F>> rangeGetter, BytesConverter<F> converter) {
        return searchRange(rangeGetter, converter::toBytes);
    }

    public HBaseQueryMapperBuilder<T> searchPrefix(Function<T, byte[]> prefixMapper) {
        return searchRange(chain(prefixMapper, start -> {
            byte[] stop = Bytes.unsignedCopyAndIncrement(start);
            return Pair.of(start, stop);
        }));
    }

    public <F> HBaseQueryMapperBuilder<T> searchPrefix(Function<T, F> prefixGetter, Function<F, byte[]> converter) {
        return searchPrefix(chain(prefixGetter, converter));
    }

    public <F> HBaseQueryMapperBuilder<T> searchPrefix(Function<T, F> prefixGetter, BytesConverter<F> converter) {
        return searchPrefix(prefixGetter, converter::toBytes);
    }

    public HBaseQueryMapperBuilder<T> searchKeySlice(int sliceSize) {
        return searchPrefix(query -> {
            byte[] rowKey = rowKeyMapper.apply(query);
            return Arrays.copyOfRange(rowKey, 0, sliceSize);
        });
    }

    public HBaseQueryMapper<T> build() {
        return new HBaseQueryMapper<T>() {
            @Override
            public @NotNull Set<byte[]> prefixes() {
                return prefixes;
            }

            @Override
            public @NotNull Set<byte[]> qualifiers() {
                return qualifiers;
            }

            @Override
            public byte[] toRowKey(T query) {
                return rowKeyMapper.apply(query);
            }

            @Override
            public List<Pair<byte[], byte[]>> toSearchRanges(T query) {
                return rangeMapper.apply(query);
            }
        };
    }
}
