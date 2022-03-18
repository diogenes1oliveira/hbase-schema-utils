package hbase.schema.connector.services;

import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.connector.interfaces.HBaseQueryBuilder;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class HBaseCellsQueryBuilder<T> implements HBaseQueryBuilder<T> {
    private final int scanKeySize;
    private final HBaseMutationMapper<T> mutationMapper;

    public HBaseCellsQueryBuilder(int scanKeySize, HBaseMutationMapper<T> mutationMapper) {
        this.scanKeySize = scanKeySize;
        this.mutationMapper = mutationMapper;
    }

    @Override
    public byte @Nullable [] toRowKey(T query) {
        return mutationMapper.toRowKey(query);
    }

    @Override
    public MultiRowRangeFilter toMultiRowRangeFilter(List<? extends T> queries) {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        for (T query : queries) {
            byte[] rowKey = toRowKey(query);
            if (rowKey == null) {
                continue;
            }
            byte[] scanStart = Arrays.copyOfRange(rowKey, 0, scanKeySize);
            byte[] scanStop = Bytes.unsignedCopyAndIncrement(scanStart);
            ranges.add(new MultiRowRangeFilter.RowRange(
                    scanStart, true, scanStop, false
            ));
        }

        return new MultiRowRangeFilter(ranges);
    }

    @Override
    public @NotNull Set<byte[]> getQualifiers(T query) {
        return mutationMapper.qualifiers();
    }

    @Override
    public @NotNull Set<byte[]> getPrefixes(T query) {
        return mutationMapper.prefixes();
    }
}
