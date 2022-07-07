package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseGenericRow;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * Schema to query and parse data fetched from HBase using standard Java objects
 *
 * @param <Q> query object type
 * @param <R> result object type
 */
public interface HBaseReadSchema<Q, R> {
    /**
     * Generates the list of Scans corresponding to the query object
     *
     * @param query query object
     * @return generated Scans
     * @throws UnsupportedOperationException schema doesn't support this operation
     */
    default List<Scan> toScans(Q query) {
        throw new UnsupportedOperationException("Multiple Scan not supported by this schema: " + this.getClass().getName());
    }

    /**
     * Generates a Get corresponding to the query object
     *
     * @param query query object
     * @return generated Get
     * @throws UnsupportedOperationException schema doesn't support this operation
     */
    default Get toGet(Q query) {
        throw new UnsupportedOperationException("Get not supported by this schema: " + this.getClass().getName());
    }

    /**
     * Generates a Filter corresponding to the query object
     * <p>
     * The default implementation just returns {@code null}
     *
     * @param query query object
     * @return generated Filter or null
     */
    default Filter toFilter(Q query) {
        return null;
    }

    /**
     * Builds a fresh instance of the result object.
     * <p>
     * Generally, this will be something like {@code ResultClass::new}
     */
    R newInstance();

    /**
     * Populates the result object with data from the row key
     * <p>
     * The default implementation just returns {@code false}, signaling no data was parsed
     *
     * @param result target result object
     * @param row    fetched row
     * @param query  original query object
     * @return {@code true} if some data was parsed
     */
    default boolean parseRow(R result, HBaseGenericRow row, Q query) {
        return false;
    }

    /**
     * Validates whether the result parsed from the row is valid.
     * <p>
     * The default implementation always returns {@code true}
     *
     * @param result result object
     * @param query  original query object
     * @return {@code true} if the parsed result is valid
     */
    default boolean validate(R result, Q query) {
        return true;
    }

}
