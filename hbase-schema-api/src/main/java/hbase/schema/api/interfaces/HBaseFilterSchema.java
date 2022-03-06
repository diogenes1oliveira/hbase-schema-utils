package hbase.schema.api.interfaces;

import hbase.schema.api.utils.HBaseSchemaUtils;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Stream;

import static hbase.schema.api.utils.HBaseSchemaUtils.combineNullableFilters;

/**
 * Interface to generate HBase Filters from Java objects
 *
 * @param <T> query object type
 */
public interface HBaseFilterSchema<T> {
    /**
     * Builds a filter that selects the columns in the result
     *
     * @param query query object
     * @return constructed filter or null
     */
    @Nullable Filter buildColumnFilter(T query);

    /**
     * Builds a filter that selects the rows in the result using a query object
     * to get the filter parameters
     * <p>
     * The default implementation returns {@code null}
     *
     * @param query query object
     * @return constructed filter or null
     */
    @Nullable
    default Filter buildRowFilter(T query) {
        return null;
    }

    /**
     * Builds a filter that selects the rows in the result using a collection of query objects
     * to get the filter parameters
     * <p>
     * The default implementation combines the filters from {@link #buildRowFilter(Object)} using
     * {@link FilterList.Operator#MUST_PASS_ONE}
     *
     * @param queries query objects
     * @return constructed filter or null
     */
    @Nullable
    default Filter buildRowFilter(List<? extends T> queries) {
        Stream<Filter> filters = queries.stream().map(this::buildRowFilter);
        return HBaseSchemaUtils.combineNullableFilters(FilterList.Operator.MUST_PASS_ONE, filters.iterator());
    }

    /**
     * Builds a filter that selects rows and columns using a query object to get the filter
     * parameters
     * <p>
     * The default implementation combines the results of {@link #buildRowFilter(Object)} and
     * {@link #buildColumnFilter(Object)}
     * using {@code FilterList.Operator.MUST_PASS_ALL}
     *
     * @param query query object
     * @return constructed filter or null
     */
    @Nullable
    default Filter buildFilter(T query) {
        Filter columnFilter = buildColumnFilter(query);
        Filter rowFilter = buildRowFilter(query);

        return combineNullableFilters(FilterList.Operator.MUST_PASS_ALL, columnFilter, rowFilter);
    }

    /**
     * Builds a filter that selects rows and columns using a query object to get the filter
     * parameters
     * <p>
     * The default implementation combines the results of {@link #buildRowFilter(List)} and {@link #buildColumnFilter(Object)}
     * using {@code FilterList.Operator.MUST_PASS_ALL}
     *
     * @param queries query objects
     * @return constructed filter or null
     */
    @Nullable
    default Filter buildFilter(List<? extends T> queries) {
        if (queries.isEmpty()) {
            return null;
        }
        Filter columnFilter = buildColumnFilter(queries.get(0));
        Filter rowFilter = buildRowFilter(queries);

        return combineNullableFilters(FilterList.Operator.MUST_PASS_ALL, columnFilter, rowFilter);
    }
}
