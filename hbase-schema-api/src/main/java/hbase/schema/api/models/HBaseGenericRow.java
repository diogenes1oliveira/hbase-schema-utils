package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Collections.emptyList;

/**
 * Data for a complete HBase row
 */
public class HBaseGenericRow {
    private static final byte[] EMPTY = new byte[0];
    private final Long timestamp;
    private final SortedSet<HBaseValueCell> valueCells;
    private final SortedSet<HBaseDeltaCell> longCells;
    private byte[] rowKey;

    /**
     * @param rowKey     {@link #getRowKey()}
     * @param timestamp  {@link #getTimestamp()}
     * @param valueCells {@link #getValueCells()}
     * @param longCells  {@link #getLongCells()}
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public HBaseGenericRow(@JsonProperty("row_key") byte[] rowKey,
                           @JsonProperty("timestamp") @Nullable Long timestamp,
                           @JsonProperty("values") Collection<HBaseValueCell> valueCells,
                           @JsonProperty("longs") Collection<HBaseDeltaCell> longCells) {
        this.rowKey = rowKey;
        this.timestamp = timestamp;
        this.valueCells = new TreeSet<>(valueCells);
        this.longCells = new TreeSet<>(longCells);
    }

    /**
     * Default constructor
     */
    public HBaseGenericRow() {
        this(EMPTY, null, emptyList(), emptyList());
    }

    /**
     * Row key bytes
     */
    @JsonProperty("row_key")
    public byte[] getRowKey() {
        return rowKey;
    }

    /**
     * {@link #getRowKey()}
     */
    @JsonIgnore
    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    /**
     * Row timestamp in milliseconds
     */
    @JsonProperty("timestamp")
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * Row {@code byte[]} cells
     */
    @JsonProperty("values")
    public SortedSet<HBaseValueCell> getValueCells() {
        return valueCells;
    }

    /**
     * Row {@code Long} cells
     */
    @JsonProperty("longs")
    public SortedSet<HBaseDeltaCell> getLongCells() {
        return longCells;
    }

    @Override
    public String toString() {
        Instant instant = this.timestamp != null ? Instant.ofEpochMilli(this.timestamp) : null;

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("row_key", Bytes.toStringBinary(rowKey))
                .append("timestamp", instant)
                .append("values", valueCells)
                .append("longs", longCells)
                .toString();
    }

}
