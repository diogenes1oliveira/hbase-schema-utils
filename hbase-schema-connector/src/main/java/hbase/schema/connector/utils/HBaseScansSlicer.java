package hbase.schema.connector.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static hbase.schema.api.utils.ScanComparator.SCAN_COMPARATOR;
import static hbase.schema.connector.utils.HBaseQueryUtils.toHBaseShell;
import static java.util.stream.Collectors.toList;

public class HBaseScansSlicer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScansSlicer.class);

    private final TableName tableName;
    private final List<Scan> scans;

    public HBaseScansSlicer(TableName tableName, List<Scan> scans) {
        this.tableName = tableName;
        this.scans = new ArrayList<>(scans);
        this.scans.sort(SCAN_COMPARATOR);
    }

    public List<Scan> getScans() {
        return this.scans;
    }

    public void forEach(Consumer<Scan> consumer) {
        this.scans.forEach(consumer);
    }

    public void removeBefore(byte[] rowKey) {
        this.scans.removeIf(scan -> scanStopsBefore(scan, rowKey));

        for (int i = 0; i < this.scans.size(); ++i) {
            Scan scan = this.scans.get(i);
            if (rowKey != null && scanIntersects(scan, rowKey)) {
                this.scans.set(i, scan.withStartRow(rowKey));
            }
        }
    }

    /**
     * Checks if the Scan stops before the given row key
     * <p>
     * This returning {@code true} means the Scan can't possibly yield values for the row key downwards
     *
     * @param scan   scan object
     * @param rowKey stop row key
     * @return {@code true} if the Scan stops before the row key
     */
    public static boolean scanStopsBefore(Scan scan, byte[] rowKey) {
        if (rowKey == null) {
            // for pagination convenience. If I don't know the search row key, any Scan might reach it
            return false;
        }
        byte[] stopRow = scan.getStopRow();
        if (stopRow == null || stopRow.length == 0) {
            // spans the whole key space
            return false;
        }

        return Bytes.compareTo(stopRow, 0, stopRow.length, rowKey, 0, stopRow.length) <= 0;
    }

    /**
     * Checks if the Scan range intersects the search row key
     * <p>
     * This returning {@code true} means the Scan could possibly yield values with the given row key
     *
     * @param scan   scan object
     * @param rowKey search row key
     * @return {@code true} if the Scan range includes the row key
     */
    public static boolean scanIntersects(Scan scan, byte[] rowKey) {
        byte[] startRow = scan.getStartRow();
        if (startRow == null || startRow.length == 0) {
            // spans the whole key space
            return true;
        }

        if (rowKey == null) {
            // for pagination convenience. If I don't know the search row key, any Scan might reach it
            return true;
        }

        byte[] stopRow = scan.getStopRow();
        if (stopRow == null || stopRow.length == 0) {
            // spans the whole key space. Not sure if necessary, given that I've already checked the start row...
            return true;
        }

        int resultStart = Bytes.compareTo(startRow, rowKey);
        int resultStop = Bytes.compareTo(rowKey, 0, stopRow.length, stopRow, 0, stopRow.length);
        return resultStart <= 0 && resultStop <= 0;
    }

    @Override
    public String toString() {
        return "HBaseScansSlicer{" +
                "this.scans=" + this.scans.stream().map(s -> toHBaseShell(s, tableName)).collect(toList()) +
                '}';
    }
}
