package hbase.schema.connector;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

public final class HBaseSchemaUtils {

    public static void selectColumns(Get get,
                                     byte[] family,
                                     SortedSet<byte[]> qualifiers,
                                     SortedSet<byte[]> qualifierPrefixes) {
        if (qualifierPrefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                get.addColumn(family, qualifier);
            }
        } else {
            get.addFamily(family);
        }
    }

    public static void selectColumns(Scan scan,
                                     byte[] family,
                                     SortedSet<byte[]> qualifiers,
                                     SortedSet<byte[]> qualifierPrefixes) {
        if (qualifierPrefixes.isEmpty()) {
            for (byte[] qualifier : qualifiers) {
                scan.addColumn(family, qualifier);
            }
        } else {
            scan.addFamily(family);
        }
    }

    public static MultiRowRangeFilter toMultiRowRangeFilter(Iterator<byte[]> it) {
        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

        while (it.hasNext()) {
            byte[] prefixStart = it.next();
            if (prefixStart == null) {
                throw new IllegalArgumentException("No search key generated for query");
            }
            byte[] prefixStop = Bytes.incrementBytes(prefixStart, 1);
            MultiRowRangeFilter.RowRange range = new MultiRowRangeFilter.RowRange(prefixStart, true, prefixStop, false);
            ranges.add(range);
        }

        return new MultiRowRangeFilter(ranges);
    }
}
