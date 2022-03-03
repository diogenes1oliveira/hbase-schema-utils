package hbase.schema.api.interfaces.converters;

import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static java.util.Optional.ofNullable;

/**
 * Interface to extract one {@code long} value from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseLongGetter<T> extends HBaseBytesGetter<T> {
    @Nullable
    Long getLong(T obj);

    @Override
    default byte @Nullable [] getBytes(T obj) {
        Long l = getLong(obj);
        return l == null ? null : Bytes.toBytes(l);
    }

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

    /**
     * Builds a new {@link HBaseLongGetter} object from a Long field
     *
     * @param getter lambda to get the Long value from the object
     * @param <T>    object type
     * @return bytes getter instance
     */
    static <T> HBaseLongGetter<T> longGetter(Function<T, Long> getter) {
        return getter::apply;
    }
}
