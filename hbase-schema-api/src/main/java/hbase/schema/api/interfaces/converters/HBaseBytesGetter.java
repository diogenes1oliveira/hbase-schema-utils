package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.function.Function;

/**
 * Interface to extract binary data from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseBytesGetter<T> {
    /**
     * @param obj POJO to get data from
     * @return binary data based on the POJO fields
     */
    @Nullable
    byte[] getBytes(T obj);

    /**
     * Convenience method to build a new byte[] getter from lambdas
     *
     * @param getter    gets the field value from the object
     * @param converter converts the field to a byte[] value
     * @param <T>       object type
     * @param <F>       field type
     * @return a new byte[] getter
     */
    static <T, F> HBaseBytesGetter<T> bytesGetter(Function<T, F> getter, Function<F, byte[]> converter) {
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
     * @param converter converts the field to a byte[] value
     * @param <T>       object type
     * @param <F>       field type
     * @return a new byte[] getter
     */
    @SuppressWarnings("unchecked")
    static <T, F> HBaseBytesGetter<T> bytesGetter(Class<F> type, Function<T, ?> getter, Function<F, byte[]> converter) {
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
