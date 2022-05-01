package hbase.schema.api.utils;

import org.apache.hadoop.hbase.client.Scan;

import java.util.Comparator;

import static hbase.schema.api.utils.BytesNullableComparator.BYTES_NULLABLE_COMPARATOR;

public class ScanComparator implements Comparator<Scan> {
    public static final ScanComparator SCAN_COMPARATOR = new ScanComparator();

    private ScanComparator() {
        // singleton
    }

    @Override
    public int compare(Scan s1, Scan s2) {
        return BYTES_NULLABLE_COMPARATOR.compare(s1.getStartRow(), s2.getStartRow());
    }
}
