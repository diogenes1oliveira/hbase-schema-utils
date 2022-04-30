package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface HBaseByteParser<R> {
    /**
     * Dummy byte parser
     */
    HBaseByteParser<Object> DUMMY = (value, result) -> {
        // nothing to do
    };

    /**
     * Populates the result object with data from a fetched cell
     *
     * @param value  fetched cell value
     * @param result result object
     * @throws IllegalArgumentException unrecognized column value
     */
    void parse(R result, ByteBuffer value);

    /**
     * Returns a dummy parser, i.e., one with a no-op for {@link #parse(Object, ByteBuffer)}
     *
     * @param <R> result object type
     * @return new dummy parser
     */
    @SuppressWarnings("unchecked")
    static <R> HBaseByteParser<R> dummy() {
        return (HBaseByteParser<R>) DUMMY;
    }

}
