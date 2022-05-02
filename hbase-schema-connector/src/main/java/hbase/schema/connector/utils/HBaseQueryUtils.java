package hbase.schema.connector.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public final class HBaseQueryUtils {
    private HBaseQueryUtils() {
        // utility class
    }


    /**
     * Combines the filters, skipping the null ones
     *
     * @param operator operator to combine the filters into a {@link FilterList}
     * @param filters  iterator that yields the (potentially null) filters to be combined
     * @return a FilterList, the only non-null Filter or null if no valid filter was found
     */
    @Nullable
    public static Filter combineNullableFilters(FilterList.Operator operator, Filter... filters) {
        FilterList filterList = new FilterList(operator);
        for (Filter filter : filters) {
            if (filter != null) {
                filterList.addFilter(filter);
            }
        }

        switch (filterList.size()) {
            case 0:
                return null;
            case 1:
                return filterList.getFilters().get(0);
            default:
                return filterList;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }

    public static String toHBaseShell(Scan scan, TableName tableName) {
        String expr = String.format("scan '%s'", tableName);
        List<String> wheres = new ArrayList<>();

        if (scan.getLimit() >= 0) {
            wheres.add(String.format("LIMIT => %d", scan.getLimit()));
        } else {
            wheres.add("LIMIT => 50");
        }
        if (scan.getTimeRange() != null) {
            wheres.add(String.format("TIMERANGE => [%s, %s]", scan.getTimeRange().getMin(), scan.getTimeRange().getMax()));
        }
        if (scan.getStartRow() != null) {
            wheres.add(String.format("STARTROW => \"%s\"", toStringBinary(scan.getStartRow())));
        }
        if (scan.getStopRow() != null) {
            wheres.add(String.format("STOPROW => \"%s\"", toStringBinary(scan.getStopRow())));
        }

        expr += ", { " + String.join(", ", wheres) + " }";

        return " " + expr + " ";
    }

    public static String toHBaseShell(Scan scan, String tableName) {
        return toHBaseShell(scan, TableName.valueOf(tableName));
    }

    public static BigInteger toBigInteger(byte[] bytes, int length, byte pad) {
        if (bytes == null) {
            bytes = new byte[0];
        }
        byte[] slice = new byte[length];
        Arrays.fill(slice, pad);

        System.arraycopy(bytes, 0, slice, 0, Math.min(length, bytes.length));

        return new BigInteger(+1, slice);
    }

    public static BigInteger toBigInteger(ByteBuffer byteBuffer, int length, byte pad) {
        if (byteBuffer == null) {
            return toBigInteger(new byte[0], length, pad);
        } else {
            return toBigInteger(Bytes.toBytes(byteBuffer), length, pad);
        }
    }

}
