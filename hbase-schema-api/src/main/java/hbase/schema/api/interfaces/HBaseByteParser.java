package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Function;

@FunctionalInterface
public interface HBaseByteParser<R> {
    /**
     * Dummy byte parser that always returns {@code false}
     */
    HBaseByteParser<Object> DUMMY = (result, value) -> false;

    /**
     * Populates the result object with data from a fetched cell
     *
     * @param value  fetched cell value
     * @param result result object
     * @return {@code true} if some data was parsed
     * @throws IllegalArgumentException unrecognized column value
     */
    boolean parse(R result, ByteBuffer value);

    /**
     * Returns a no-op parser
     *
     * @param <R> result object type
     * @return new dummy parser that always returns {@code false}
     */
    @SuppressWarnings("unchecked")
    static <R> HBaseByteParser<R> dummy() {
        return (HBaseByteParser<R>) DUMMY;
    }

    /**
     * Builds a byte parser that automatically converts the binary value and handles {@code null} accordingly
     *
     * @param setter    lambda to process the converted value
     * @param converter converts to the desired value type
     * @param <R>       result object type
     * @param <T>       converted value type
     * @return new byte parser
     */
    static <R, T> HBaseByteParser<R> hBaseByteParser(BiConsumer<R, T> setter, Function<ByteBuffer, T> converter) {
        return (result, value) -> {
            if (value == null) {
                return false;
            }
            T converted = converter.apply(value);
            if (converted == null) {
                return false;
            }
            setter.accept(result, converted);
            return true;
        };
    }
}
