package hbase.schema.api.interfaces.converters;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Optional.ofNullable;


/**
 * Interface to populate a Java object with data parsed from a {@code byte[]} value
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseBytesSetter<T> {
    void setFromBytes(T obj, byte[] bytes);

    /**
     * Dummy parser that does not do anything
     */
    static <T> HBaseBytesSetter<T> dummy() {
        return ((obj, bytes) -> {

        });
    }

    /**
     * Builds a new {@link HBaseBytesSetter} object for a bytes-conversible field
     *
     * @param setter    lambda to set the field value in the object
     * @param converter lambda to convert the {@code byte[]} value into the field type
     * @param <T>       object type
     * @param <F>       field type
     * @return bytes setter instance
     */
    static <T, F> HBaseBytesSetter<T> bytesSetter(BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return ((obj, bytes) ->
                ofNullable(bytes).map(converter).ifPresent(f -> setter.accept(obj, f))
        );
    }
}
