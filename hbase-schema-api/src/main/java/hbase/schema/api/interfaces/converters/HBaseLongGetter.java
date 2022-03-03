package hbase.schema.api.interfaces.converters;

import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static java.util.Optional.ofNullable;

/**
 * Interface to extract one {@code long} value from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseLongGetter<T> {
    @Nullable
    Long getLong(T obj);

    /**
     * Builds a new {@link HBaseLongGetter} object from a Long-conversible field
     *
     * @param getter    lambda to get the field value from the object
     * @param converter lambda to convert the field into a {@code Long}
     * @param <T>       object type
     * @param <F>       field type
     * @return bytes getter instance
     */
    static <T, F> HBaseLongGetter<T> longGetter(Function<T, F> getter, Function<F, Long> converter) {
        return obj -> ofNullable(getter.apply(obj)).map(converter).orElse(null);
    }
}
