package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.function.Function;

/**
 * Interface to extract a long value from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseLongGetter<T> extends HBaseBytesGetter<T> {
    /**
     * @param obj POJO to get data from
     * @return long data based on the POJO fields
     */
    @Nullable
    Long getLong(T obj);

    /**
     * Gets the long value using {@link #getLong(T)} and then converts it into a binary value
     *
     * @param obj POJO to get data from
     * @return binary data based on the POJO fields
     */
    @Override
    default byte[] getBytes(T obj) {
        Long l = getLong(obj);
        if (l != null) {
            return Bytes.toBytes(l);
        } else {
            return null;
        }
    }

    /**
     * Convenience method to build a new getter from lambdas
     *
     * @param getter    gets the field value from the object
     * @param converter converts the field to a Long value
     * @param <T>       object type
     * @param <F>       field type
     * @return a new long getter
     */
    static <T, F> HBaseLongGetter<T> longGetter(Function<T, F> getter, Function<F, Long> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }

    /**
     * Convenience method to build a new getter from lambdas
     *
     * @param type      field type to be checked using {@link Class#isAssignableFrom(Class)}
     * @param getter    gets the field value from the object
     * @param converter converts the field to a Long value
     * @param <T>       object type
     * @param <F>       field type
     * @return a new long getter
     */
    @SuppressWarnings("unchecked")
    static <T, F> HBaseLongGetter<T> longGetter(Class<F> type, Function<T, ?> getter, Function<F, Long> converter) {
        return obj -> {
            Object value = getter.apply(obj);
            if (value == null) {
                return null;
            } else if (!type.isAssignableFrom(value.getClass())) {
                throw new IllegalArgumentException("Incompatible type");
            }
            return converter.apply((F) value);
        };
    }
}
