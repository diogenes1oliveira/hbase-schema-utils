package hbase.schema.api.interfaces;

import hbase.base.interfaces.TriConsumer;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Interface to populate a result object with data from a single HBase cell
 *
 * @param <R> result object type
 */
@FunctionalInterface
public interface HBaseCellParser<R> {
    /**
     * Dummy cell parser that always returns {@code false}
     */
    HBaseCellParser<Object> DUMMY = (result, column, value) -> false;

    /**
     * Populates the result object with data from a fetched cell
     *
     * @param result result object
     * @param column fetched column
     * @param value  fetched cell value
     * @return {@code true} if some data was parsed
     * @throws IllegalArgumentException unrecognized column value
     */
    boolean parse(R result, ByteBuffer column, ByteBuffer value);

    /**
     * Returns a no-op parser
     *
     * @param <R> result object type
     * @return new dummy parser that always returns {@code false}
     */
    @SuppressWarnings("unchecked")
    static <R> HBaseCellParser<R> dummy() {
        return (HBaseCellParser<R>) DUMMY;
    }

    /**
     * Builds a cell parser that automatically converts the binary values and handles {@code null} accordingly
     *
     * @param setter          lambda to process the converted column and values
     * @param columnConverter converts to the desired column type
     * @param valueConverter  converts to the desired value type
     * @param <R>             result object type
     * @param <C>             converted column type
     * @param <V>             converted value type
     * @return new cell parser
     */
    static <R, C, V> HBaseCellParser<R> hBaseCellParser(TriConsumer<R, C, V> setter,
                                                        Function<ByteBuffer, C> columnConverter,
                                                        Function<ByteBuffer, V> valueConverter) {
        return (result, columnBuffer, valueBuffer) -> {
            if (columnBuffer == null || valueBuffer == null) {
                return false;
            }
            C column = columnConverter.apply(columnBuffer);
            V value = valueConverter.apply(valueBuffer);
            if (column == null || value == null) {
                return false;
            }
            setter.accept(result, column, value);
            return true;
        };
    }
}
