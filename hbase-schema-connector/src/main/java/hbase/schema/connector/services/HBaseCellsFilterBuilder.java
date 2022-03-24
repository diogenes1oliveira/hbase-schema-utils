package hbase.schema.connector.services;

import hbase.schema.api.interfaces.HBaseQueryMapper;
import hbase.schema.connector.interfaces.HBaseFilterBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public class HBaseCellsFilterBuilder<T> implements HBaseFilterBuilder<T> {
    private final HBaseQueryMapper<T> queryMapper;

    public HBaseCellsFilterBuilder(HBaseQueryMapper<T> queryMapper) {
        this.queryMapper = queryMapper;
    }

    @Override
    public byte @Nullable [] toRowKey(T query) {
        return queryMapper.toRowKey(query);
    }

    @Override
    public MultiRowRangeFilter toMultiRowRangeFilter(List<? extends T> queries) {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        for (T query : queries) {
            for (Pair<byte[], byte[]> range : queryMapper.toSearchRanges(query)) {
                byte[] start = range.getLeft();
                byte[] stop = range.getRight();
                ranges.add(new MultiRowRangeFilter.RowRange(start, true, stop, false));
            }
        }

        return new MultiRowRangeFilter(ranges);
    }

    /**
     * Selects the columns returned in a Get query
     * <p>
     * The default implementation:
     * <li>Selects the fixed columns in {@link HBaseQueryMapper#qualifiers()} if {@link HBaseQueryMapper#prefixes()} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param get    HBase Get instance
     */
    @Override
    public void selectColumns(T query, byte[] family, Get get) {
        if (queryMapper.prefixes().isEmpty()) {
            for (byte[] qualifier : queryMapper.qualifiers()) {
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
     * <li>Selects the fixed columns in {@link HBaseQueryMapper#qualifiers()} if {@link HBaseQueryMapper#prefixes()} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param scan   HBase Scan instance
     */
    @Override
    public void selectColumns(T query, byte[] family, Scan scan) {
        if (queryMapper.prefixes().isEmpty()) {
            for (byte[] qualifier : queryMapper.qualifiers()) {
                scan.addColumn(family, qualifier);
            }
        } else {
            scan.addFamily(family);
        }

    }
}
