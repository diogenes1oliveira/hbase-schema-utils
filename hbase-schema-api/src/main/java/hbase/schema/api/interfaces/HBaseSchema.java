package hbase.schema.api.interfaces;

/**
 * Interface to map Java objects to and from HBase
 *
 * @param <T> mutation type
 * @param <Q> query and filter type
 * @param <R> result type
 */
public interface HBaseSchema<T, Q, R> {
    /**
     * Schema to generate the Mutations
     */
    HBaseMutationSchema<T> mutationSchema();

    /**
     * Schema to generate the Gets and Scans queries
     */
    HBaseQuerySchema<Q> querySchema();

    /**
     * Schema to generate the Filters
     */
    HBaseFilterSchema<Q> filterSchema();

    /**
     * Schema to parse fetched Results
     */
    HBaseResultParserSchema<R> resultParserSchema();
}
