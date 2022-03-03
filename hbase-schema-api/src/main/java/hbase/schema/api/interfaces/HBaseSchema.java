package hbase.schema.api.interfaces;

/**
 * Interface to map Java objects to and from HBase
 *
 * @param <T> query and mutation types
 * @param <R> output result type
 */
public interface HBaseSchema<T, R> {
    /**
     * Schema to generate the Gets and Scans queries
     */
    HBaseQuerySchema<T> querySchema();

    /**
     * Schema to generate the Mutations
     */
    HBaseMutationSchema<T> mutationSchema();

    /**
     * Schema to parse fetched Results
     */
    HBaseResultParserSchema<R> resultParserSchema();
}
