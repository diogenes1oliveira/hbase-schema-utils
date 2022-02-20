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
public interface HBaseLongExtractor<T> extends HBaseBytesExtractor<T> {
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
     * Convenience method to build a new extractor from lambdas
     *
     * @param getter    gets the field value from the object
     * @param converter converts the field to a Long value
     * @param <T>       object type
     * @param <F>       field type
     * @return a new long extractor
     */
    static <T, F> HBaseLongExtractor<T> longGetter(Function<T, F> getter, Function<F, Long> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }
}
