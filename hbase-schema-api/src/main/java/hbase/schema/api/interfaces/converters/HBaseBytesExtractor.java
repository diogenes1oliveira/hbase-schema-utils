package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.function.Function;

/**
 * Interface to extract binary data from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseBytesExtractor<T> {
    /**
     * @param obj POJO to get data from
     * @return binary data based on the POJO fields
     */
    @Nullable
    byte[] getBytes(T obj);

    /**
     * Convenience method to build a new extractor from lambdas
     *
     * @param getter    gets the field value from the object
     * @param converter converts the field to a byte[] value
     * @param <T>       object type
     * @param <F>       field type
     * @return a new bytes extractor
     */
    static <T, F> HBaseBytesExtractor<T> bytesGetter(Function<T, F> getter, Function<F, byte[]> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }
}
