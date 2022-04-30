package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Decorator/Proxy for {@link HBaseReadSchema} instances
 *
 * @param <Q> query object type
 * @param <R> result object type
 */
public abstract class HBaseReadSchemaWrapper<Q, R> implements HBaseReadSchema<Q, R> {
    private final HBaseReadSchema<Q, R> wrapped;

    /**
     * @param wrapped wrapped instance
     */
    protected HBaseReadSchemaWrapper(HBaseReadSchema<Q, R> wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Generates the list of Scans corresponding to the query object
     *
     * @param query query object
     * @return generated Scans
     * @throws UnsupportedOperationException schema doesn't support this operation
     */
    public List<Scan> toScans(Q query) {
        return wrapped.toScans(query);
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Generates a Get corresponding to the query object
     *
     * @param query query object
     * @return generated Get
     * @throws UnsupportedOperationException schema doesn't support this operation
     */
    public Get toGet(Q query) {
        return wrapped.toGet(query);
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Builds a fresh instance of the result object.
     * <p>
     * Generally, this will be something like {@code ResultClass::new}
     */
    public R newInstance() {
        return wrapped.newInstance();
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Populates the result object with data from the row key
     * <p>
     * The default implementation just returns {@code false}, signaling no data was parsed
     *
     * @param rowKey fetched row key
     * @param query  original query object
     * @return {@code true} if some data was parsed
     */
    public boolean parseRowKey(R result, ByteBuffer rowKey, Q query) {
        return wrapped.parseRowKey(result, rowKey, query);
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Populates the result object with data from a fetched cell
     *
     * @param result    result object
     * @param qualifier fetched column qualifier
     * @param value     fetched cell value
     * @param query     original query object
     * @return {@code true} if some data was parsed
     */
    public boolean parseCell(R result, ByteBuffer qualifier, ByteBuffer value, Q query) {
        return wrapped.parseCell(result, qualifier, value, query);
    }
}
