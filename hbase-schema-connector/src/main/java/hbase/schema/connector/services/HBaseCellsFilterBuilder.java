package hbase.schema.connector.services;

import hbase.schema.api.interfaces.HBaseCellsMapper;
import hbase.schema.api.interfaces.HBaseRowMapper;
import hbase.schema.connector.interfaces.HBaseFilterBuilder;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class HBaseCellsFilterBuilder<T> implements HBaseFilterBuilder<T> {
    private final int scanKeySize;
    private final HBaseRowMapper<T> rowMapper;
    private final HBaseCellsMapper<T> cellsMapper;

    public HBaseCellsFilterBuilder(int scanKeySize, HBaseRowMapper<T> rowMapper, HBaseCellsMapper<T> cellsMapper) {
        this.scanKeySize = scanKeySize;
        this.rowMapper = rowMapper;
        this.cellsMapper = cellsMapper;
    }

    @Override
    public byte @Nullable [] toRowKey(T query) {
        return rowMapper.toRowKey(query);
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
        return cellsMapper.qualifiers();
    }

    @Override
    public @NotNull Set<byte[]> getPrefixes(T query) {
        return cellsMapper.prefixes();
    }
}
