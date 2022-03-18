package hbase.schema.connector.utils;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.jetbrains.annotations.Nullable;

public final class HBaseQueryUtils {
    private HBaseQueryUtils() {
        // utility class
    }


    /**
     * Combines the filters, skipping the null ones
     *
     * @param operator operator to combine the filters into a {@link FilterList}
     * @param filters  iterator that yields the (potentially null) filters to be combined
     * @return a FilterList, the only non-null Filter or null if no valid filter was found
     */
    @Nullable
    public static Filter combineNullableFilters(FilterList.Operator operator, Filter... filters) {
        FilterList filterList = new FilterList(operator);
        for (Filter filter : filters) {
            if (filter != null) {
                filterList.addFilter(filter);
            }
        }

        switch (filterList.size()) {
            case 0:
                return null;
            case 1:
                return filterList.getFilters().get(0);
            default:
                return filterList;
        }
    }

}
