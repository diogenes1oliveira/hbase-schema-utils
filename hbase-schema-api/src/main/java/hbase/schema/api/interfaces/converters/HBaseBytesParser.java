package hbase.schema.api.interfaces.converters;

/**
 * Interface to parse data from a byte[] value into a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseBytesParser<T> {
    /**
     * @param pojo  POJO instance to set the fields of
     * @param value data from row key, qualifier or cell value
     */
    void setFromBytes(T pojo, byte[] value);

    /**
     * Returns a no-op parser
     *
     * @param <T> input object type
     * @return new dummy bytes parser
     */
    static <T> HBaseBytesParser<T> dummy() {
        return (obj, bytes) -> {

        };
    }
}
