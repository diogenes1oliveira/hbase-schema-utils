package hbase.schema.connector.services;

import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.connector.interfaces.HBaseFilterBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HBaseCellsFilterBuilder<T> implements HBaseFilterBuilder<T> {
    private final int scanKeySize;
    private final HBaseMutationMapper<T> mutationMapper;
    private final Set<byte[]> qualifiers;
    private final Set<byte[]> prefixes;

    public HBaseCellsFilterBuilder(int scanKeySize, HBaseMutationMapper<T> mutationMapper) {
        this.scanKeySize = scanKeySize;
        this.mutationMapper = mutationMapper;
        this.prefixes = new HashSet<>();
        this.qualifiers = new HashSet<>();
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

    /**
     * Selects the columns returned in a Get query
     * <p>
     * The default implementation:
     * <li>Selects the fixed columns in {@link HBaseMutationMapper#qualifiers()} if {@link HBaseMutationMapper#prefixes()} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param get    HBase Get instance
     */
    @Override
    public void selectColumns(T query, byte[] family, Get get) {
        if (prefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                get.addColumn(family, qualifier);
            }
        } else {
            get.addFamily(family);
        }
    }

    /**
     * Selects the columns returned in a Scan query
     * <p>
     * The default implementation:
     * <li>Selects the fixed columns in {@link HBaseMutationMapper#qualifiers()} if {@link HBaseMutationMapper#prefixes()} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param scan   HBase Scan instance
     */
    @Override
    public void selectColumns(T query, byte[] family, Scan scan) {
        if (prefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                scan.addColumn(family, qualifier);
            }
        } else {
            scan.addFamily(family);
        }

    }
}
