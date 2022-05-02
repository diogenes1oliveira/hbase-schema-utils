package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.filter.Filter;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface HBaseFilterBuilder<Q> {
    /**
     * Builds a filter to based on a query object
     * <p>
     * The default implementation just returns {@code null}
     *
     * @param query input query objects
     * @return filter for HBase query requests or {@code null} if no Filter could be generated
     */
    @Nullable Filter toFilter(Q query);

}
