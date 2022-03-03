package hbase.schema.connector;

import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.connector.interfaces.HBaseFilterGenerator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds HBase filters for Get and Scan based on a schema
 *
 * @param <T> query object type
 */
public class HBaseSchemaFilterGenerator<T> implements HBaseFilterGenerator<T> {
    private final HBaseQuerySchema<T> querySchema;

    /**
     * @param querySchema query schema object
     */
    public HBaseSchemaFilterGenerator(HBaseQuerySchema<T> querySchema) {
        this.querySchema = querySchema;
    }

    /**
     * Builds a filter for the column prefixes, if any is set in {@link HBaseQuerySchema#getPrefixes(T)}
     *
     * @param query query object
     * @return filter for the column prefixes or null
     */
    @Nullable
    @Override
    public Filter toFilter(T query) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (byte[] prefix : querySchema.getPrefixes(query)) {
            if (prefix.length > 0) {
                Filter qualifierFilter = new ColumnPrefixFilter(prefix);
                filterList.addFilter(qualifierFilter);
            }
        }
        if (filterList.size() == 0) {
            return null;
        } else {
            return filterList;
        }
    }

    /**
     * Builds a {@link MultiRowRangeFilter} for each query scan key.
     * <p>
     * Combines with {@link #toFilter(T)} if it's non-null
     *
     * @param queries list of query objects
     * @return filter for the column prefixes or null
     */
    @Nullable
    @Override
    public Filter toFilter(List<? extends T> queries) {
        if (queries.isEmpty()) {
            return null;
        }
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        for (T query : queries) {
            byte[] scanStart = querySchema.buildScanKey(query);
            byte[] scanStop = Bytes.unsignedCopyAndIncrement(scanStart);
            ranges.add(new MultiRowRangeFilter.RowRange(
                    scanStart, true, scanStop, false
            ));
        }

        MultiRowRangeFilter rowRangeFilter = new MultiRowRangeFilter(ranges);
        Filter columnFilter = toFilter(queries.get(0));
        if (columnFilter != null) {
            return new FilterList(FilterList.Operator.MUST_PASS_ALL, rowRangeFilter, columnFilter);
        } else {
            return rowRangeFilter;
        }
    }

}
