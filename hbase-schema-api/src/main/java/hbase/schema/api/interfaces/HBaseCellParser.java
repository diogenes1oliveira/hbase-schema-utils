package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;

/**
 * Interface to populate a result object with data from a single HBase cell
 *
 * @param <R> result object type
 */
@FunctionalInterface
public interface HBaseCellParser<R> {
    /**
     * Dummy cell parser
     */
    HBaseCellParser<Object> DUMMY = (result, column, value) -> {
        // nothing to do
    };

    /**
     * Populates the result object with data from a fetched cell
     *
     * @param result result object
     * @param column fetched column
     * @param value  fetched cell value
     * @throws IllegalArgumentException unrecognized column value
     */
    void parse(R result, ByteBuffer column, ByteBuffer value);

    /**
     * Returns a dummy parser, i.e., one with a no-op for {@link #parse(Object, ByteBuffer, ByteBuffer)}
     *
     * @param <R> result object type
     * @return new dummy parser
     */
    @SuppressWarnings("unchecked")
    static <R> HBaseCellParser<R> dummy() {
        return (HBaseCellParser<R>) DUMMY;
    }

}
