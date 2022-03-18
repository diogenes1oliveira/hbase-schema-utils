package hbase.schema.api.interfaces;

/**
 * Interface to map Java objects to and from HBase
 *
 * @param <T> mutation and query type
 * @param <R> result type
 */
public interface HBaseSchema<T, R> {
    /**
     * Schema to generate the Mutations
     */
    HBaseMutationMapper<T> mutationMapper();

    /**
     * Schema to generate the Gets and Scans queries
     */
    HBaseResultParser<R> resultParser();

    /**
     * Size of scan key
     */
    int scanKeySize();
}
