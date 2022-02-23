package hbase.schema.api.interfaces;

public interface HBaseSchema<T, R> {
    HBaseQuerySchema<T> querySchema();

    HBaseMutationSchema<T> mutationSchema();

    HBaseResultParser<R> resultParser();
}
