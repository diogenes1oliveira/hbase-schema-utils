package hbase.schema.api.interfaces;

/**
 * Interface to populate the POJO with data from the fetched qualifiers and values
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseFromCellSetter<T> {
    /**
     * @param pojo      POJO object
     * @param qualifier column qualifier
     * @param value     cell vale
     */
    void parse(T pojo, byte[] qualifier, byte[] value);
}
