package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseGenericRow;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

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
    @Override
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
    @Override
    public Get toGet(Q query) {
        return wrapped.toGet(query);
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Generates a Filter corresponding to the query object
     * <p>
     * The default implementation just returns {@code null}
     *
     * @param query query object
     * @return generated Filter or null
     */
    @Override
    public Filter toFilter(Q query) {
        return wrapped.toFilter(query);
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Builds a fresh instance of the result object.
     * <p>
     * Generally, this will be something like {@code ResultClass::new}
     */
    @Override
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
     * @param result target result object
     * @param row    fetched row
     * @param query  original query object
     * @return {@code true} if some data was parsed
     */
    @Override
    public boolean parseRow(R result, HBaseGenericRow row, Q query) {
        return wrapped.parseRow(result, row, query);
    }

    /**
     * Description copied from {@link HBaseReadSchema}
     * <p>
     * Validates whether the result parsed from the row is valid.
     * <p>
     * The default implementation always returns {@code true}
     *
     * @param result result object
     * @param query  original query object
     * @return {@code true} if the parsed result is valid
     */
    @Override
    public boolean validate(R result, Q query) {
        return wrapped.validate(result, query);
    }

}
