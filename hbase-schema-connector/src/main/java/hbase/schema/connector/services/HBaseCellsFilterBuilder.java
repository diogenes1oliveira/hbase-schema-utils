package hbase.schema.connector.services;

import hbase.schema.api.interfaces.HBaseQueryMapper;
import hbase.schema.connector.interfaces.HBaseFilterBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.bytesCollectionToString;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseCellsFilterBuilder<T> implements HBaseFilterBuilder<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseCellsFilterBuilder.class);

    private final HBaseQueryMapper<T> queryMapper;

    public HBaseCellsFilterBuilder(HBaseQueryMapper<T> queryMapper) {
        this.queryMapper = queryMapper;
    }

    @Override
    public byte @Nullable [] toRowKey(T query) {
        byte[] rowKey = queryMapper.toRowKey(query);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting rowKey = {}", toStringBinary(rowKey));
        }
        return rowKey;
    }

    @Override
    public MultiRowRangeFilter toMultiRowRangeFilter(List<? extends T> queries) {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        for (T query : queries) {
            for (Pair<byte[], byte[]> range : queryMapper.toSearchRanges(query)) {
                byte[] start = range.getLeft();
                byte[] stop = range.getRight();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Scanning range {} - {}", toStringBinary(start), toStringBinary(stop));
                }
                ranges.add(new MultiRowRangeFilter.RowRange(start, true, stop, false));
            }
        }

        if (!ranges.isEmpty()) {
            return new MultiRowRangeFilter(ranges);
        } else {
            LOGGER.debug("No MultiRowRangeFilter for there are no ranges");
            return null;
        }
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
        Set<byte[]> prefixes = queryMapper.prefixes();
        Set<byte[]> qualifiers = queryMapper.qualifiers();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Selecting columns in Get: prefixes={}, qualifiers={}",
                    bytesCollectionToString(prefixes),
                    bytesCollectionToString(qualifiers)
            );
        }
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
     * <li>Selects the fixed columns in {@link HBaseQueryMapper#qualifiers()} if {@link HBaseQueryMapper#prefixes()} is empty;</li>
     * <li>Otherwise, selects the whole family.</li>
     *
     * @param query  query object
     * @param family column family
     * @param scan   HBase Scan instance
     */
    @Override
    public void selectColumns(T query, byte[] family, Scan scan) {
        Set<byte[]> prefixes = queryMapper.prefixes();
        Set<byte[]> qualifiers = queryMapper.qualifiers();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Selecting columns in Scan: prefixes={}, qualifiers={}",
                    bytesCollectionToString(prefixes),
                    bytesCollectionToString(qualifiers)
            );
        }
        if (prefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                scan.addColumn(family, qualifier);
            }
        } else {
            scan.addFamily(family);
        }

    }
}
