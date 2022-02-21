package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface to extract a long value from a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseLongMapper<T> extends HBaseBytesMapper<T> {
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

    static <T> HBaseLongMapper<T> readOnly() {
        return obj -> {
            throw new UnsupportedOperationException("Read-only field");
        };
    }
}
