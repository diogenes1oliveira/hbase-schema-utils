package hbase.schema.api.models;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.builder.ToStringStyle;

import java.time.Instant;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class HBaseGenericBase64Row {
    private String rowKey;
    private Long timestampMs;
    private Map<String, String> bytesCells;
    private Map<String, Long> longCells;

    public HBaseGenericBase64Row(String rowKey,
                                 @Nullable Long timestampMs,
                                 Map<String, String> bytesCells,
                                 @Nullable Map<String, Long> longCells) {
        this.rowKey = rowKey;
        this.timestampMs = timestampMs;
        this.bytesCells = bytesCells;
        this.longCells = longCells;
    }

    public HBaseGenericBase64Row(String rowKey,
                                 @Nullable Long timestampMs,
                                 Map<String, String> bytesCells) {
        this(rowKey, timestampMs, bytesCells, emptyMap());
    }

    public HBaseGenericBase64Row(String rowKey,
                                 Map<String, String> bytesCells) {
        this(rowKey, null, bytesCells);
    }

    public HBaseGenericBase64Row(String rowKey) {
        this(rowKey, null, emptyMap());
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    @Nullable
    public Long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(@Nullable Long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public Map<String, String> getBytesCells() {
        return bytesCells;
    }

    public void setBytesCells(Map<String, String> bytesCells) {
        this.bytesCells = bytesCells;
    }

    @Nullable
    public Map<String, Long> getLongCells() {
        return longCells;
    }

    public void setLongCells(@Nullable Map<String, Long> longCells) {
        this.longCells = longCells;
    }

    @Override
    public String toString() {
        Instant timestamp = timestampMs != null ? Instant.ofEpochMilli(timestampMs) : null;

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("row_key", rowKey)
                .append("timestamp", timestamp)
                .append("bytes", bytesCells)
                .append("longs", longCells)
                .toString();
    }

}
