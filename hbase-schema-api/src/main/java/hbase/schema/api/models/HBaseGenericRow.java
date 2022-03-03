package hbase.schema.api.models;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

public class HBaseGenericRow {
    private byte[] rowKey;
    private Long timestampMs;
    private NavigableMap<byte[], byte[]> bytesCells;
    private NavigableMap<byte[], Long> longCells;

    public HBaseGenericRow(byte[] rowKey,
                           @Nullable Long timestampMs,
                           NavigableMap<byte[], byte[]> bytesCells,
                           @Nullable NavigableMap<byte[], Long> longCells) {
        this.rowKey = rowKey;
        this.timestampMs = timestampMs;
        this.bytesCells = bytesCells;
        this.longCells = longCells;
    }

    public HBaseGenericRow(byte[] rowKey,
                           @Nullable Long timestampMs,
                           NavigableMap<byte[], byte[]> bytesCells) {
        this(rowKey, timestampMs, bytesCells, asBytesTreeMap());
    }

    public HBaseGenericRow(byte[] rowKey,
                           NavigableMap<byte[], byte[]> bytesCells) {
        this(rowKey, null, bytesCells);
    }

    public HBaseGenericRow(byte[] rowKey) {
        this(rowKey, null, asBytesTreeMap());
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

    public NavigableMap<byte[], byte[]> getBytesCells() {
        return bytesCells;
    }

    public void setBytesCells(NavigableMap<byte[], byte[]> bytesCells) {
        this.bytesCells = bytesCells;
    }

    @Nullable
    public NavigableMap<byte[], Long> getLongCells() {
        return longCells;
    }

    public void setLongCells(@Nullable NavigableMap<byte[], Long> longCells) {
        this.longCells = longCells;
    }

    @Override
    public String toString() {
        Instant timestamp = timestampMs != null ? Instant.ofEpochMilli(timestampMs) : null;

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("row_key", Bytes.toStringBinary(rowKey))
                .append("timestamp", timestamp)
                .append("bytes", new PrettyBytesMap(bytesCells))
                .append("longs", new PrettyLongMap(longCells))
                .toString();
    }

}
