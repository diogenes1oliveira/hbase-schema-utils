package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.NavigableMap;

import static java.util.Collections.emptyList;

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
     * The default implementation just delegates to the row timestamp via {@link #buildTimestamp(Object)}
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
    NavigableMap<byte[], byte[]> buildPutValues(T object);

    /**
     * Builds a map of Increment values based on the object fields
     *
     * @param object object to be inserted into HBase
     * @return map of (qualifier -> increment value)
     */
    NavigableMap<byte[], Long> buildIncrementValues(T object);

    default List<HBaseDeltaCell> toDeltas(T object) {
        return emptyList();
    }

    default List<HBaseValueCell> toValues(T object) {
        return emptyList();
    }
}
