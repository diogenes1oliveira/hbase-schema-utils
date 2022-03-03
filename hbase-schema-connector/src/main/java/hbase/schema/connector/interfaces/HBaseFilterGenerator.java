package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Builds HBase filters from Java objects that act as source data
 *
 * @param <T> query object type
 */
public interface HBaseFilterGenerator<T> {
    /**
     * Creates a HBase filter from a query object
     */
    @Nullable
    Filter toFilter(T query);

    /**
     * Creates a HBase filter from a list of query objects
     * <p>
     * The default implementation just creates a combined {@link FilterList} for each item in the list
     */
    @Nullable
    default Filter toFilter(List<? extends T> queries) {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        for (T query : queries) {
            list.addFilter(toFilter(query));
        }

        return list;
    }

    /**
     * Selects the columns returned in a Get query
     *
     * @param query query object
     * @param get   HBase Get instance
     */
    void selectColumns(T query, Get get);

    /**
     * Selects the columns returned in a Scan query
     *
     * @param query query object
     * @param scan  HBase Scan instance
     */
    void selectColumns(T query, Scan scan);
}
