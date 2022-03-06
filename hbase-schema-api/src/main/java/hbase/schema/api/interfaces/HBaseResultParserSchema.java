package hbase.schema.api.interfaces;

import org.jetbrains.annotations.Nullable;

import java.util.NavigableMap;

/**
 * Interface to parse raw data from HBase into Java objects
 *
 * @param <T> result object instance
 */
public interface HBaseResultParserSchema<T> {
    /**
     * Creates a new instance of the result object
     * <p>
     * The implementation is likely to be something like {@code return new SomePojo(); }
     */
    T newInstance();

    /**
     * Populates the object with data from HBase
     *
     * @param obj         result object instance
     * @param rowKey      row key bytes
     * @param resultCells cells fetched from HBase, in a mapping (qualifier -> cell value)
     * @return true if some data was set
     */
    boolean setFromResult(T obj, byte[] rowKey, NavigableMap<byte[], byte[]> resultCells);

    /**
     * Creates a new result object and populates it with data fetched from HBase
     * <p>
     * The default implementation delegates to {@link #newInstance()} and
     * {@link #setFromResult(Object, byte[], NavigableMap)}
     *
     * @param rowKey      row key bytes
     * @param resultCells cells fetched from HBase, in a mapping (qualifier -> cell value)
     * @return parsed result object or null if no data was set
     */
    default @Nullable T parseResult(byte[] rowKey, NavigableMap<byte[], byte[]> resultCells) {
        T instance = newInstance();
        if (setFromResult(instance, rowKey, resultCells)) {
            return instance;
        } else {
            return null;
        }
    }

}
