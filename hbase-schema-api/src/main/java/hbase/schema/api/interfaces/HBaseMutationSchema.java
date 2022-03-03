package hbase.schema.api.interfaces;

import org.jetbrains.annotations.Nullable;

import java.util.NavigableMap;

/**
 * Interface to generate the Puts and Increments corresponding to a Java object
 *
 * @param <T> object type
 */
public interface HBaseMutationSchema<T> {
    /**
     * Builds the row key for this object
     * <p>
     * If {@code null}, the object is skipped
     *
     * @param object object to be inserted into HBase
     * @return row key bytes
     */
    byte @Nullable [] buildRowKey(T object);

    /**
     * Builds the timestamp for this object
     * <p>
     * If {@code null}, the object is skipped
     *
     * @param object object to be inserted into HBase
     * @return timestamp in milliseconds
     */
    @Nullable
    Long buildTimestamp(T object);

    /**
     * Builds the timestamp for a single cell
     * <p>
     * The default implementation just delegates to the row timestamp via {@link #buildTimestamp(T)}
     *
     * @param object    object to be inserted into HBase
     * @param qualifier column qualifier
     * @return timestamp in milliseconds for the cell
     */
    @Nullable
    default Long buildTimestamp(T object, byte[] qualifier) {
        return buildTimestamp(object);
    }

    /**
     * Builds a map of Put values based on the object fields
     *
     * @param object object to be inserted into HBase
     * @return map of (qualifier -> cell value)
     */
    NavigableMap<byte[], byte[]> buildCellValues(T object);

    /**
     * Builds a map of Increment values based on the object fields
     *
     * @param object object to be inserted into HBase
     * @return map of (qualifier -> increment value)
     */
    NavigableMap<byte[], Long> buildCellIncrements(T object);
}
