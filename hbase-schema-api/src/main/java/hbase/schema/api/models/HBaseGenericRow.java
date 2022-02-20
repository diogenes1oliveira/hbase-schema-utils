package hbase.schema.api.models;

import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.SortedMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.sortedByteMap;

public class HBaseGenericRow {
    private byte[] rowKey;
    private Long timestampMs;
    private SortedMap<byte[], byte[]> bytesCells;
    private SortedMap<byte[], Long> longCells;

    public HBaseGenericRow(byte[] rowKey,
                           @Nullable Long timestampMs,
                           SortedMap<byte[], byte[]> bytesCells,
                           @Nullable SortedMap<byte[], Long> longCells) {
        this.rowKey = rowKey;
        this.timestampMs = timestampMs;
        this.bytesCells = bytesCells;
        this.longCells = longCells;
    }

    public HBaseGenericRow(byte[] rowKey,
                           @Nullable Long timestampMs,
                           SortedMap<byte[], byte[]> bytesCells) {
        this(rowKey, timestampMs, bytesCells, sortedByteMap());
    }

    public HBaseGenericRow(byte[] rowKey,
                           SortedMap<byte[], byte[]> bytesCells) {
        this(rowKey, null, bytesCells);
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    @Nullable
    public Long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(@Nullable Long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public SortedMap<byte[], byte[]> getBytesCells() {
        return bytesCells;
    }

    public void setBytesCells(SortedMap<byte[], byte[]> bytesCells) {
        this.bytesCells = bytesCells;
    }

    @Nullable
    public SortedMap<byte[], Long> getLongCells() {
        return longCells;
    }

    public void setLongCells(@Nullable SortedMap<byte[], Long> longCells) {
        this.longCells = longCells;
    }
}
