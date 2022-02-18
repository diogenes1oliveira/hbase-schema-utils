package com.github.diogenes1oliveira.hbase.schema.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Interface to generate a Scan corresponding to a POJO query
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseScanGenerator<T> {

    /**
     * Builds a Scan request based on the data from the POJO objects
     *
     * @param queries    POJO objects to act as query source data
     * @param readSchema object to generate the Scan data from the POJO
     * @return built Get request
     */
    Scan toScan(Collection<? extends T> queries, HBaseReadSchema<T> readSchema);

    /**
     * Generates a filter to select the row keys in a Scan request
     * <p>
     * The default implementation generates a {@link MultiRowRangeFilter} based on the search prefix
     * generated from {@link HBaseReadSchema#getScanRowKeyGenerator()}
     *
     * @param queries    POJO objects to act as query source data
     * @param readSchema object to generate the qualifier filter data
     * @return built filter for the qualifiers or null
     */
    @Nullable
    default Filter toRowKeyScanFilter(Collection<? extends T> queries, HBaseReadSchema<T> readSchema) {
        HBaseBytesExtractor<T> prefixGenerator = readSchema.getScanRowKeyGenerator();
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        for (T query : queries) {
            byte[] prefixStart = prefixGenerator.getBytes(query);
            if (prefixStart == null) {
                throw new IllegalArgumentException("No search key generated for query");
            }
            byte[] prefixStop = Bytes.incrementBytes(prefixStart, 1);
            MultiRowRangeFilter.RowRange range = new MultiRowRangeFilter.RowRange(prefixStart, true, prefixStop, false);
            ranges.add(range);
        }

        return new MultiRowRangeFilter(ranges);
    }
}
