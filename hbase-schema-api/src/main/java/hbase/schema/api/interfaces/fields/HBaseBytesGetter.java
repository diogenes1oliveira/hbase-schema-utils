package hbase.schema.api.interfaces.fields;

import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static java.util.Optional.ofNullable;

/**
 * Interface to extract one {@code byte[]} value from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseBytesGetter<T> {
    byte @Nullable [] getBytes(T obj);

    /**
     * Builds a new {@link HBaseBytesGetter} object from a bytes-conversible field
     *
     * @param getter    lambda to get the field value from the object
     * @param converter lambda to convert the field into a {@code byte[]}
     * @param <T>       object type
     * @param <F>       field type
     * @return bytes getter instance
     */
    static <T, F> HBaseBytesGetter<T> bytesGetter(Function<T, F> getter, Function<F, byte[]> converter) {
        return obj -> ofNullable(getter.apply(obj)).map(converter).orElse(null);
    }
}
