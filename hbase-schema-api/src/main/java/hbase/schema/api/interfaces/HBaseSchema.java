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
     * Schema to parse the results from queries
     */
    HBaseResultParser<R> resultParser();

    /**
     * Schema to generate queries
     */
    HBaseQueryMapper<T> queryMapper();

    /**
     * Name to identify this schema
     * <p>
     * The default implementation returns the class' simple name
     */
    default String name() {
        return this.getClass().getSimpleName();
    }
}
