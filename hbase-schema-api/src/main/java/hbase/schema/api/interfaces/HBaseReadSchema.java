package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
     * Generates a Scan corresponding to the query object
     * <p>
     * The default implementation generates a Scan with a MultiRowRangeFilter combining the ones from {@link #toScans(Object)}
     *
     * @param query query object
     * @return generated Scan
     * @throws UnsupportedOperationException schema doesn't support this operation
     */
    default Scan toScan(Q query) {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        for (Scan scan : toScans(query)) {
            MultiRowRangeFilter.RowRange range = new MultiRowRangeFilter.RowRange(scan.getStartRow(), true, scan.getStopRow(), false);
            ranges.add(range);
        }

        Scan scan = new Scan();
        scan.setFilter(new MultiRowRangeFilter(ranges));
        return scan;
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
     * @param rowKey fetched row key
     * @param query  original query object
     * @return {@code true} if some data was parsed
     */
    default boolean parseRowKey(R result, ByteBuffer rowKey, Q query) {
        return false;
    }

    /**
     * Populates the result object with data from a fetched cell
     *
     * @param result    result object
     * @param qualifier fetched column qualifier
     * @param value     fetched cell value
     * @param query     original query object
     * @return {@code true} if some data was parsed
     */
    boolean parseCell(R result, ByteBuffer qualifier, ByteBuffer value, Q query);
}
