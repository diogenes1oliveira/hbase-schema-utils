package hbase.schema.api.interfaces.converters;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface to parse data from a byte[] value into a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseLongParser<T> extends HBaseBytesParser<T> {

    Long ZERO = 0L;

    /**
     * @param pojo  POJO instance to set the fields of
     * @param value data from long cell value
     */
    void setFromLong(T pojo, long value);

    /**
     * @param pojo  POJO instance to set the fields of
     * @param value data from row key, qualifier or cell value
     * @throws IllegalArgumentException input bytes has a length different from 0 or 8
     */
    default void setFromBytes(T pojo, byte[] value) {
        if (value.length == 0) {
            setFromLong(pojo, ZERO);
        } else if (value.length == 8) {
            setFromLong(pojo, Bytes.toLong(value));
        } else {
            throw new IllegalArgumentException("Invalid long value");
        }
    }

    /**
     * Returns a no-op parser
     *
     * @param <T> input object type
     * @return new dummy long parser
     */
    static <T> HBaseLongParser<T> dummy() {
        return (obj, l) -> {

        };
    }
}
