package hbase.schema.api.utils;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;

import java.util.Comparator;

import static hbase.schema.api.utils.BytesNullableComparator.BYTES_NULLABLE_COMPARATOR;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;
import static java.util.Comparator.comparing;

public class ScanComparator {
    public static final Comparator<Scan> SCAN_COMPARATOR = comparing(Scan::getStartRow, BYTES_NULLABLE_COMPARATOR)
            .thenComparing(Scan::getStopRow, BYTES_NULLABLE_COMPARATOR.reversed())
            .thenComparing(chain(Scan::getTimeRange, TimeRange::getMin, ScanComparator::safeLong))
            .thenComparing(comparing(chain(Scan::getTimeRange, TimeRange::getMin, ScanComparator::safeLong)).reversed())
            .thenComparing(Scan::getLimit);

    private ScanComparator() {
        // singleton
    }

    private static long safeLong(Long value) {
        return value == null ? 0L : value;
    }

}
